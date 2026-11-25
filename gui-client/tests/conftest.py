import os
import threading
from concurrent import futures

import grpc
import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from fossildb_gui.api import pb, pb_grpc  # noqa: E402


class FakeFossilDB(pb_grpc.FossilDBServicer):
    """In-memory FossilDB. With legacy=True, it lacks GetDatabaseInfo like servers before it was added."""

    def __init__(self, legacy: bool = False):
        self.legacy = legacy
        self.data: dict[str, dict[str, dict[int, bytes]]] = {"collectionA": {}, "collectionB": {}}
        self.compactions = 0
        self.multiple_versions_requests = 0
        self.info_requests = 0
        # Cleared to hold back ListKeys replies
        self.list_keys_gate = threading.Event()
        self.list_keys_gate.set()

    def put(self, collection: str, key: str, version: int, value: bytes) -> None:
        self.data[collection].setdefault(key, {})[version] = value

    def _store(self, collection, context_reply):
        if collection not in self.data:
            raise KeyError(f"java.util.NoSuchElementException: No store for column family {collection}")
        return self.data[collection]

    def Health(self, request, context):
        return pb.HealthReply(success=True)

    def GetDatabaseInfo(self, request, context):
        self.info_requests += 1
        if self.legacy:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not found")
        collections = [
            pb.CollectionInfoProto(
                name=name,
                columnFamily=name,
                estimatedVersionCount=sum(len(v) for v in keys.values()),
                sstFileCount=1,
                totalSstFilesBytes=1000,
                liveSstFilesBytes=1000,
                estimatedLiveDataBytes=1000,
                sstEntries=10,
                sstDeletions=0,
                rawKeyBytes=100,
                rawValueBytes=4000,
                indexBytes=10,
                filterBytes=0,
                memtableBytes=0,
                memtableEntries=0,
                memtableDeletions=0,
                tableReadersMemoryBytes=100,
                estimatedPendingCompactionBytes=0,
                compactionPending=False,
                levels=[pb.LevelInfoProto(level=6, fileCount=1, sizeBytes=1000, compressions=["ZSTD"])],
                **({"firstKey": min(keys), "lastKey": max(keys)} if keys else {}),
            )
            for name, keys in self.data.items()
        ]
        return pb.GetDatabaseInfoReply(
            success=True,
            databaseInfo=pb.DatabaseInfoProto(
                fossildbVersion="test",
                rocksdbVersion="9.4.0",
                startTimeMillis=0,
                dataDir="/data",
                backupDir="/backup",
                diskTotalBytes=10**12,
                diskUsableBytes=10**11,
                walSizeBytes=0,
                backupInProgress=False,
                compactionInProgress=False,
                writesStopped=False,
                delayedWriteRateBytesPerSecond=0,
                backgroundErrors=0,
                runningCompactions=0,
                runningFlushes=0,
                collections=collections,
            ),
        )

    def ListKeys(self, request, context):
        self.list_keys_gate.wait(10)
        try:
            keys = sorted(self._store(request.collection, None))
        except KeyError as e:
            return pb.ListKeysReply(success=False, errorMessage=str(e))
        if request.HasField("prefix"):
            keys = [k for k in keys if k.startswith(request.prefix)]
        if request.HasField("startAfterKey"):
            keys = [k for k in keys if k > request.startAfterKey]
        if request.HasField("limit"):
            keys = keys[: request.limit]
        return pb.ListKeysReply(success=True, keys=keys)

    def ListVersions(self, request, context):
        versions = self.data[request.collection].get(request.key, {})
        return pb.ListVersionsReply(success=True, versions=sorted(versions, reverse=True))

    def Get(self, request, context):
        versions = self.data[request.collection].get(request.key, {})
        if request.version not in versions:
            return pb.GetReply(success=False, errorMessage="No such element", value=b"", actualVersion=0)
        return pb.GetReply(success=True, value=versions[request.version], actualVersion=request.version)

    def GetMultipleVersions(self, request, context):
        self.multiple_versions_requests += 1
        versions = self.data[request.collection].get(request.key, {})
        newest = request.newestVersion if request.HasField("newestVersion") else max(versions, default=0)
        oldest = request.oldestVersion if request.HasField("oldestVersion") else 0
        chosen = sorted((v for v in versions if oldest <= v <= newest), reverse=True)
        return pb.GetMultipleVersionsReply(success=True, versions=chosen, values=[versions[v] for v in chosen])

    def Delete(self, request, context):
        self.data[request.collection].get(request.key, {}).pop(request.version, None)
        if not self.data[request.collection].get(request.key, True):
            del self.data[request.collection][request.key]
        return pb.DeleteReply(success=True)

    def DeleteMultipleVersions(self, request, context):
        self.data[request.collection].pop(request.key, None)
        return pb.DeleteMultipleVersionsReply(success=True)

    def DeleteAllByPrefix(self, request, context):
        store = self.data[request.collection]
        for key in [k for k in store if k.startswith(request.prefix)]:
            del store[key]
        return pb.DeleteAllByPrefixReply(success=True)

    def CompactAllData(self, request, context):
        self.compactions += 1
        return pb.CompactAllDataReply(success=True)


def _serve(fake: FakeFossilDB):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_FossilDBServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}"


@pytest.fixture(params=[False, True], ids=["current", "legacy"])
def fake_server(request):
    fake = FakeFossilDB(legacy=request.param)
    server, address = _serve(fake)
    yield fake, address
    server.stop(None)


@pytest.fixture
def current_server():
    fake = FakeFossilDB()
    server, address = _serve(fake)
    yield fake, address
    server.stop(None)


@pytest.fixture(scope="session")
def qt_app():
    from PySide6.QtCore import QCoreApplication
    from PySide6.QtWidgets import QApplication

    QCoreApplication.setOrganizationName("scalableminds-tests")
    QCoreApplication.setApplicationName("fossildb-gui-tests")
    app = QApplication.instance() or QApplication([])
    yield app
