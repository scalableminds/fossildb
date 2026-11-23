package com.scalableminds.fossildb

import java.io.File
import java.nio.file.Paths
import com.scalableminds.fossildb.db.{RocksDBManager, RocksDBStore, VersionedKeyValueStore}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

class WriteBatchSuite extends AnyFlatSpec with BeforeAndAfterEach with TestHelpers {

  private val testTempDir = "testData3"
  private val dataDir = Paths.get(testTempDir, "data")
  private val collectionA = "collectionA"

  private var managerOpt: Option[RocksDBManager] = None
  private def store: RocksDBStore = managerOpt.get.getStoreForColumnFamily(collectionA).get

  private val value1 = "value1".getBytes
  private val value2 = "value2".getBytes

  override def beforeEach(): Unit = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()
    managerOpt = Some(new RocksDBManager(dataDir, List(collectionA), None))
  }

  override def afterEach(): Unit = {
    managerOpt.foreach(_.close())
    managerOpt = None
    deleteRecursively(new File(testTempDir))
  }

  "withWriteBatch" should "write nothing if the block throws after some writes" in {
    store.put("existing", value1)
    assertThrows[IllegalStateException] {
      store.withWriteBatch { batch =>
        batch.put("new", value2)
        batch.delete("existing")
        throw new IllegalStateException("fail mid-batch")
      }
    }
    assert(store.get("new") == null)
    assert(store.get("existing") sameElements value1)
  }

  it should "apply all puts and deletes once the block completes" in {
    store.put("existing", value1)
    store.withWriteBatch { batch =>
      batch.put("new", value2)
      batch.delete("existing")
    }
    assert(store.get("new") sameElements value2)
    assert(store.get("existing") == null)
  }

  "deleteMultipleVersions" should "delete stored keys whose version suffix differs from the canonical format" in {
    // A canonical key would be "aKey@FFFFFFFFFFFFFFFA@5", rebuilding it from the parsed version misses this one
    val nonCanonicalKey = "aKey@legacySuffix@5"
    store.put(nonCanonicalKey, value1)
    val versionedStore = new VersionedKeyValueStore(store)
    versionedStore.withRawRocksIterator(rocksIt => versionedStore.deleteMultipleVersions(rocksIt, "aKey"))
    assert(store.get(nonCanonicalKey) == null)
  }

}
