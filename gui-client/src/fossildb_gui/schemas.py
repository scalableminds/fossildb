"""Loading .proto schema files from disk at runtime, to decode values with their message types."""

import importlib.resources
import os
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import Descriptor

# protoc writes its errors to the process's stderr, which is redirected while it runs, so only one run at a time
_protoc_lock = threading.Lock()


class SchemaError(Exception):
    pass


@dataclass
class SchemaSet:
    folder: Path
    pool: descriptor_pool.DescriptorPool
    message_names: list[str]  # all message types, sorted, without map entries and well-known types

    def message_class(self, full_name: str):
        try:
            descriptor = self.pool.FindMessageTypeByName(full_name)
        except KeyError:
            raise SchemaError(f"Message type {full_name} is not defined in {self.folder}") from None
        return message_factory.GetMessageClass(descriptor)


def load_schemas(folder: Path) -> SchemaSet:
    """Compiles all .proto files in the folder (recursively), with the folder as include path."""
    folder = folder.resolve()
    files = sorted(p.relative_to(folder).as_posix() for p in folder.rglob("*.proto"))
    if not files:
        raise SchemaError(f"No .proto files found in {folder}")
    # The well-known types (google/protobuf/*.proto) ship with grpcio-tools
    well_known_types = importlib.resources.files("grpc_tools") / "_proto"
    with tempfile.TemporaryDirectory() as tmp:
        output = Path(tmp) / "descriptors.pb"
        exit_code, errors = _run_protoc(
            [
                "protoc",
                f"-I{folder}",
                f"-I{well_known_types}",
                f"--descriptor_set_out={output}",
                "--include_imports",
                *files,
            ]
        )
        if exit_code != 0:
            raise SchemaError(f"Compiling the .proto files in {folder} failed:\n{errors.strip()}")
        descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(output.read_bytes())

    pool = descriptor_pool.DescriptorPool()
    for file in descriptor_set.file:  # with --include_imports, dependencies come first
        pool.Add(file)

    names: list[str] = []

    def collect(descriptor: Descriptor) -> None:
        if not descriptor.GetOptions().map_entry:
            names.append(descriptor.full_name)
        for nested in descriptor.nested_types:
            collect(nested)

    for file in descriptor_set.file:
        if file.package == "google.protobuf":
            continue
        for descriptor in pool.FindFileByName(file.name).message_types_by_name.values():
            collect(descriptor)
    return SchemaSet(folder, pool, sorted(names))


def _run_protoc(args: list[str]) -> tuple[int, str]:
    from grpc_tools import protoc

    with _protoc_lock, tempfile.TemporaryFile() as captured:
        sys.stderr.flush()
        saved_stderr = os.dup(2)
        try:
            os.dup2(captured.fileno(), 2)
            exit_code = protoc.main(args)
        finally:
            os.dup2(saved_stderr, 2)
            os.close(saved_stderr)
        captured.seek(0)
        return exit_code, captured.read().decode(errors="replace")
