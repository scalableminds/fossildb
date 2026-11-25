"""The FossilDB gRPC API, generated from ../src/main/protobuf/fossildbapi.proto.

The generated modules are not checked in. They are (re)generated on import when the .proto file is newer, using the
protoc bundled with grpcio-tools, so they always match the installed protobuf runtime. Packaged builds contain the
generated modules and no .proto file, so nothing is generated there.
"""

from pathlib import Path

_PACKAGE_DIR = Path(__file__).parent
_SOURCE_ROOT = _PACKAGE_DIR.parent.parent  # the src directory
_PROTO_FILE = _SOURCE_ROOT.parent.parent / "src" / "main" / "protobuf" / "fossildbapi.proto"
_GENERATED = [_PACKAGE_DIR / "fossildbapi_pb2.py", _PACKAGE_DIR / "fossildbapi_pb2_grpc.py"]


def _needs_generation() -> bool:
    if not _PROTO_FILE.exists():
        return False
    proto_mtime = _PROTO_FILE.stat().st_mtime
    return any(not f.exists() or f.stat().st_mtime < proto_mtime for f in _GENERATED)


def _generate() -> None:
    from grpc_tools import protoc

    # Mapping the proto directory to the package path makes the generated grpc module import its messages
    # as fossildb_gui.api.fossildbapi_pb2 instead of a top-level module
    exit_code = protoc.main(
        [
            "protoc",
            f"-Ifossildb_gui/api={_PROTO_FILE.parent}",
            f"--python_out={_SOURCE_ROOT}",
            f"--grpc_python_out={_SOURCE_ROOT}",
            "fossildb_gui/api/fossildbapi.proto",
        ]
    )
    if exit_code != 0:
        raise RuntimeError(f"Generating the FossilDB API code from {_PROTO_FILE} failed")


if _needs_generation():
    _generate()

from fossildb_gui.api import fossildbapi_pb2 as pb  # noqa: E402
from fossildb_gui.api import fossildbapi_pb2_grpc as pb_grpc  # noqa: E402

__all__ = ["pb", "pb_grpc"]
