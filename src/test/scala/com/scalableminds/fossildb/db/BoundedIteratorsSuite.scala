package com.scalableminds.fossildb.db

import java.io.File
import java.nio.file.Paths
import com.scalableminds.fossildb.TestHelpers
import org.rocksdb.RocksIterator
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

class BoundedIteratorsSuite extends AnyFlatSpec with BeforeAndAfterEach with TestHelpers {

  private val testTempDir = "testData3"
  private val columnFamily = "collectionA"

  private var managerOpt: Option[RocksDBManager] = None
  private def rocksStore: RocksDBStore = managerOpt.get.getStoreForColumnFamily(columnFamily).get
  private def store = new VersionedKeyValueStore(rocksStore)

  override def beforeEach(): Unit = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()
    managerOpt = Some(new RocksDBManager(Paths.get(testTempDir, "data"), List(columnFamily), None))
    // "keyA" is the upper bound for the versions of "key", "kez" the upper bound for keys with prefix "key"
    Seq("key", "keyA", "kez").foreach(store.put(_, 0, Array[Byte](1)))
  }

  override def afterEach(): Unit = {
    managerOpt.foreach(_.close())
    deleteRecursively(new File(testTempDir))
  }

  // Seeks to key and returns all keys the raw iterator visits, without any prefix filtering
  private def visitedKeys(rocksIt: RocksIterator, key: String): Seq[String] = {
    rocksIt.seek(RocksDBStore.encodeKey(key))
    Iterator.continually(rocksIt).takeWhile(_.isValid).map { it =>
      val visited = VersionedKey(RocksDBStore.decodeKey(it.key())).get.key
      it.next()
      visited
    }.toSeq
  }

  "Key iterators" should "stop at the last version of the key" in {
    val visited = rocksStore.withIterators(store.withKeyIterator(_, "key")(visitedKeys(_, "key")))
    assert(visited == Seq("key"))
  }

  "Prefix iterators" should "stop at the last key with the prefix" in {
    val visited = rocksStore.withIterators(store.withPrefixIterator(_, Some("key"))(visitedKeys(_, "key")))
    assert(visited == Seq("key", "keyA"))
  }

  it should "be unbounded without a prefix" in {
    val visited = rocksStore.withIterators(store.withPrefixIterator(_, None)(visitedKeys(_, "")))
    assert(visited == Seq("key", "keyA", "kez"))
  }

  "Iterators sharing read options" should "each use their own upper bound" in {
    val visited = rocksStore.withIterators { iterators =>
      Seq(
        store.withKeyIterator(iterators, "key")(visitedKeys(_, "key")),
        store.withPrefixIterator(iterators, None)(visitedKeys(_, "key")),
        store.withKeyIterator(iterators, "keyA")(visitedKeys(_, "key"))
      )
    }
    assert(visited == Seq(Seq("key"), Seq("key", "keyA", "kez"), Seq("key", "keyA")))
  }

}
