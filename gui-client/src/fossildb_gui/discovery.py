"""Finding FossilDB servers: checking whether known ones answer, and looking for ones on this computer."""

import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fossildb_gui.connection import FossilDBError, probe

PROBE_TIMEOUT_SECONDS = 2
LOCAL_PROBE_TIMEOUT_SECONDS = 1
_LISTEN = "0A"  # TCP state in /proc/net/tcp
# Listening on these, a port can be reached via localhost (all-zero means any address)
_LOCAL_V4 = re.compile(r"(00000000|[0-9A-F]{6}7F)")  # little-endian, so 127.x.x.x ends with 7F
_LOCAL_V6 = {"0" * 32, "00000000000000000000000001000000"}  # :: and ::1


def check(address: str, timeout: float = PROBE_TIMEOUT_SECONDS) -> str | None:
    """None if FossilDB answers at the address, else why not."""
    try:
        probe(address, timeout)
        return None
    except FossilDBError as e:
        return str(e)


def listening_local_ports() -> set[int]:
    """TCP ports that accept connections on localhost. Read from the system, so no ports are tried in vain."""
    if sys.platform.startswith("linux"):
        return _ports_from_proc()
    if sys.platform == "darwin":
        return _ports_from_lsof()
    return set()


def _ports_from_proc() -> set[int]:
    ports = set()
    for name, ipv6 in (("tcp", False), ("tcp6", True)):
        try:
            ports |= _parse_proc_net(Path("/proc/net", name).read_text(), ipv6)
        except OSError:
            continue
    return ports


def _parse_proc_net(text: str, ipv6: bool) -> set[int]:
    """Listening ports from the contents of /proc/net/tcp or tcp6, if they can be reached via localhost."""
    ports = set()
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 4 or fields[3] != _LISTEN:
            continue
        address, _, port = fields[1].partition(":")
        if address in _LOCAL_V6 if ipv6 else _LOCAL_V4.fullmatch(address):
            ports.add(int(port, 16))
    return ports


def _ports_from_lsof() -> set[int]:
    try:
        output = subprocess.run(
            ["lsof", "-nP", "-iTCP", "-sTCP:LISTEN"], capture_output=True, text=True, timeout=5
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return set()
    return {
        int(port)
        for host, port in re.findall(r"\s(\S+):(\d+) \(LISTEN\)", output)
        if host in ("*", "127.0.0.1", "[::1]", "localhost")
    }


def find_local(exclude: set[int]) -> list[int]:
    """Ports on this computer where FossilDB answers, e.g. a development server or one in Docker."""
    ports = sorted(listening_local_ports() - exclude)
    if not ports:
        return []
    with ThreadPoolExecutor(min(16, len(ports)), thread_name_prefix="fossildb-scan") as pool:
        answers = pool.map(lambda port: check(f"localhost:{port}", LOCAL_PROBE_TIMEOUT_SECONDS), ports)
        return [port for port, error in zip(ports, answers, strict=True) if error is None]
