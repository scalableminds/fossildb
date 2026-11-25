"""Formatting numbers, sizes and times for display."""

import datetime as dt
import re


def size(num_bytes: int | float) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError


def count(n: int) -> str:
    return f"{n:,}"


def compact_count(n: int) -> str:
    """Short counts like 870, 3.4 k, 41.3 M."""
    for limit, unit in ((10**12, "T"), (10**9, "G"), (10**6, "M"), (10**3, "k")):
        if n >= limit:
            return f"{n / limit:.1f} {unit}"
    return str(n)


def timestamp(seconds: float) -> str:
    return dt.datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M")


def date(seconds: float) -> str:
    return dt.datetime.fromtimestamp(seconds).strftime("%Y-%m-%d")


def duration(seconds: float) -> str:
    seconds = int(seconds)
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes = rest // 60
    if days:
        return f"{days} d {hours} h"
    if hours:
        return f"{hours} h {minutes} min"
    return f"{minutes} min"


# Timestamps are shown as dates when they fall into this range, as milliseconds or seconds since 1970
_PLAUSIBLE_SECONDS = (dt.datetime(1990, 1, 1, tzinfo=dt.UTC).timestamp(), dt.datetime(2100, 1, 1, tzinfo=dt.UTC).timestamp())


def iso_timestamp(value) -> str | None:
    """The number as ISO date in UTC, read as milliseconds if that is plausible, else as seconds. None if neither is."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    for seconds, milliseconds in ((value / 1000, True), (value, False)):
        if _PLAUSIBLE_SECONDS[0] <= seconds < _PLAUSIBLE_SECONDS[1]:
            precise = milliseconds or seconds != int(seconds)
            moment = dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
            return moment.isoformat(timespec="milliseconds" if precise else "seconds").replace("+00:00", "Z")
    return None


def with_timestamp(name: str, value, text: str) -> str:
    """Adds the ISO date to the text of numeric values whose name contains "timestamp"."""
    if "timestamp" not in name.lower():
        return text
    iso = iso_timestamp(value)
    return f"{text} ({iso})" if iso else text


_OBJECT_ID = re.compile(r"[0-9a-fA-F]{24}")


def object_id_date(key: str) -> str | None:
    """The creation date (UTC) encoded in a BSON ObjectId such as 5a0627196b0000221d37a590: its first 4 bytes are the
    seconds since 1970. None if the key is not an ObjectId, or the date is implausible."""
    if not _OBJECT_ID.fullmatch(key):
        return None
    seconds = int(key[:8], 16)
    if not _PLAUSIBLE_SECONDS[0] <= seconds < _PLAUSIBLE_SECONDS[1]:
        return None
    return dt.datetime.fromtimestamp(seconds, tz=dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
