package com.scalableminds.fossildb

import java.io.File
import java.nio.file.{Files, Paths}
import com.scalableminds.fossildb.db.{StoreManager, VersionedKey}
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, CompressionType, ConfigOptions, DBOptions, Env, Options, RocksDB, SstFileReader, TableProperties}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.jdk.CollectionConverters.{BufferHasAsJava, IteratorHasAsScala, ListHasAsScala, SeqHasAsJava}
import scala.util.Using


class RocksOptionsSuite extends AnyFlatSpec with BeforeAndAfterEach with TestHelpers {

  private val testTempDir = "testData2"
  private val dataDir = Paths.get(testTempDir, "data")
  private val backupDir = Paths.get(testTempDir, "backup")

  private val collectionA = "collectionA"
  private val collectionB = "collectionB"

  private val columnFamilies = List(collectionA, collectionB)


  override def beforeEach(): Unit = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()
  }

  override def afterEach(): Unit = {
    deleteRecursively(new File(testTempDir))
  }


  private def sstFileProperties(): Seq[TableProperties] =
    Files.list(dataDir).iterator().asScala.filter(_.toString.endsWith(".sst")).toSeq.map { path =>
      Using.Manager { use =>
        val reader = use(new SstFileReader(use(new Options())))
        reader.open(path.toString)
        reader.getTableProperties
      }.get
    }

  "Initializing the StoreManager" should "load and use a specified config file" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  stats_dump_period_sec=700\n\n[CFOptions \"default\"]\n\n")

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath))

    val options = new DBOptions()
      .setStatsDumpPeriodSec(100)
    val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()
    val configOptions = new ConfigOptions()
    // if successful, the rocksdb writes the loaded options to a file that can then be retrieved with loadLatestOptions
    // we test that that one now includes the value 700 from the file above, rather than the 100 specified as a default
    org.rocksdb.OptionsUtil.loadLatestOptions(configOptions, dataDir.toString, options, cfListRef.asJava)
    assert(options.statsDumpPeriodSec() == 700)
    storeManager.close
  }

  "CompactAllData" should "compact the data of all column families and drop deleted entries" in {
    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None)
    try {
      Seq(collectionA, collectionB).foreach { collection =>
        val store = storeManager.getStore(collection)
        (0 until 100).foreach(i => store.put(s"key$i", 0, Array[Byte](1)))
        store.withRawRocksIterator(it => (0 until 50).foreach(i => store.deleteMultipleVersions(it, s"key$i")))
      }
      storeManager.compactAllData()
      val store = storeManager.getStore(collectionB)
      assert(store.withRawRocksIterator(it => store.listKeys(it, None, None, None)).size == 50)
    } finally {
      storeManager.close
    }
    // Before, only the default column family was compacted, so the data stayed in the memtables
    assert(sstFileProperties().map(p => new String(p.getColumnFamilyName)).sorted == Seq(collectionA, collectionB))
    assert(sstFileProperties().map(_.getNumEntries).sum == 100)
    assert(sstFileProperties().map(_.getNumDeletions).sum == 0)
  }

  it should "rewrite bottommost files with the current options" in {
    // Write files to the bottommost level with options that differ from FossilDB's (no compression)
    RocksDB.loadLibrary()
    dataDir.toFile.mkdirs()
    Using.Manager { use =>
      val columnOptions = use(new ColumnFamilyOptions().setCompressionType(CompressionType.NO_COMPRESSION))
      val descriptors = (RocksDB.DEFAULT_COLUMN_FAMILY +: columnFamilies.map(_.getBytes)).map(new ColumnFamilyDescriptor(_, columnOptions))
      val handles = new java.util.ArrayList[ColumnFamilyHandle]
      val db = use(RocksDB.open(use(new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)), dataDir.toString, descriptors.asJava, handles))
      handles.asScala.foreach(use(_))
      db.put(handles.get(1), VersionedKey("aKey", 0).toString.getBytes, Array[Byte](1))
      db.compactRange(handles.get(1))
    }.get
    assert(sstFileProperties().map(_.getCompressionName) == Seq("NoCompression"))

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None)
    try {
      storeManager.compactAllData()
      val store = storeManager.getStore(collectionA)
      assert(store.withRawRocksIterator(it => store.get(it, "aKey", None)).exists(_.value.sameElements(Array[Byte](1))))
    } finally {
      storeManager.close
    }
    assert(sstFileProperties().map(_.getCompressionName) != Seq("NoCompression"))
  }

  it should "fail if specified config file does not exist" in {
    assertThrows[Exception] {
      new StoreManager(dataDir, backupDir, columnFamilies, Some("nonExistingPath.ini"))
    }
  }

  it should "fail if specified config file is invalid" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  stats_dump_period_sec=700")

    assertThrows[Exception] {
      new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath))
    }
  }


}
