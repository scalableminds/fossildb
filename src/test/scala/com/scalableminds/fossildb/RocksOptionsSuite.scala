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
    Using.resource(Files.list(dataDir))(_.iterator().asScala.filter(_.toString.endsWith(".sst")).toList).map { path =>
      Using.Manager { use =>
        val reader = use(new SstFileReader(use(new Options())))
        reader.open(path.toString)
        reader.getTableProperties
      }.get
    }

  "Initializing the StoreManager" should "load and use a specified config file" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  stats_dump_period_sec=700\n\n[CFOptions \"default\"]\n\n")

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath), ConfigDefaults.blockCacheSizeMB)

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

  it should "use the shared block cache with the configured size without a config file" in {
    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None, blockCacheSizeMB = 64)
    val store = storeManager.getStore(collectionA)
    store.put("aKey", 0, Array[Byte](1))
    storeManager.compactAllData()
    assert(store.withRawRocksIterator(it => store.get(it, "aKey", None)).exists(_.value.sameElements(Array[Byte](1))))
    storeManager.close

    val optionsFileContent = new String(Files.readAllBytes(Files.list(dataDir).iterator().asScala.filter(_.getFileName.toString.startsWith("OPTIONS-")).max))
    assert(optionsFileContent.contains("cache_index_and_filter_blocks=false"))
    // The cache capacity is not part of the OPTIONS file, but RocksDB logs it when opening the DB
    val logContent = new String(Files.readAllBytes(dataDir.resolve("LOG")))
    assert(logContent.contains("capacity : 64000000"))
  }

  "CompactAllData" should "rewrite bottommost files with the current options" in {
    // Write uncompressed files to the bottommost level
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

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None, ConfigDefaults.blockCacheSizeMB)
    try {
      storeManager.compactAllData()
      val store = storeManager.getStore(collectionA)
      assert(store.withRawRocksIterator(it => store.get(it, "aKey", None)).exists(_.value.sameElements(Array[Byte](1))))
    } finally {
      storeManager.close
    }
    val compressionNames = sstFileProperties().map(_.getCompressionName)
    assert(compressionNames.nonEmpty && compressionNames.forall(_ != "NoCompression"))
  }

  it should "compress with lz4, and zstd for the bottommost level, without a config file" in {
    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None, ConfigDefaults.blockCacheSizeMB)
    storeManager.close

    val options = new DBOptions()
    val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()
    org.rocksdb.OptionsUtil.loadLatestOptions(new ConfigOptions(), dataDir.toString, options, cfListRef.asJava)
    val cfOptions = cfListRef.find(cf => new String(cf.getName) == collectionA).get.getOptions
    assert(cfOptions.compressionType() == CompressionType.LZ4_COMPRESSION)
    assert(cfOptions.bottommostCompressionType() == CompressionType.ZSTD_COMPRESSION)
  }

  private def maxBackgroundJobsInOptionsFile(): Int =
    Using.resources(new DBOptions(), new ConfigOptions()) { (options, configOptions) =>
      val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()
      org.rocksdb.OptionsUtil.loadLatestOptions(configOptions, dataDir.toString, options, cfListRef.asJava)
      options.maxBackgroundJobs()
    }

  "maxBackgroundJobs" should "default to 8 without a config file" in {
    new StoreManager(dataDir, backupDir, columnFamilies, None, ConfigDefaults.blockCacheSizeMB).close
    assert(maxBackgroundJobsInOptionsFile() == 8)
  }

  it should "use the value from the config file if not set" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  max_background_jobs=3\n\n[CFOptions \"default\"]\n\n")
    new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath), ConfigDefaults.blockCacheSizeMB).close
    assert(maxBackgroundJobsInOptionsFile() == 3)
  }

  it should "override the value from the config file if set" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  max_background_jobs=3\n\n[CFOptions \"default\"]\n\n")
    new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath), ConfigDefaults.blockCacheSizeMB, maxBackgroundJobs = Some(5)).close
    assert(maxBackgroundJobsInOptionsFile() == 5)
  }

  it should "convert snappy files to zstd" in {
    // Write files with the RocksDB default compression (snappy)
    RocksDB.loadLibrary()
    dataDir.toFile.mkdirs()
    Using.Manager { use =>
      val columnOptions = use(new ColumnFamilyOptions())
      val descriptors = (RocksDB.DEFAULT_COLUMN_FAMILY +: columnFamilies.map(_.getBytes)).map(new ColumnFamilyDescriptor(_, columnOptions))
      val handles = new java.util.ArrayList[ColumnFamilyHandle]
      val db = use(RocksDB.open(use(new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)), dataDir.toString, descriptors.asJava, handles))
      handles.asScala.foreach(use(_))
      db.put(handles.get(1), VersionedKey("aKey", 0).toString.getBytes, Array[Byte](1))
      db.compactRange(handles.get(1))
    }.get
    assert(sstFileProperties().map(_.getCompressionName) == Seq("Snappy"))

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None, ConfigDefaults.blockCacheSizeMB)
    try {
      storeManager.compactAllData()
      val store = storeManager.getStore(collectionA)
      assert(store.withRawRocksIterator(it => store.get(it, "aKey", None)).exists(_.value.sameElements(Array[Byte](1))))
    } finally {
      storeManager.close
    }
    assert(sstFileProperties().map(_.getCompressionName) == Seq("ZSTD"))
  }

  it should "fail if specified config file does not exist" in {
    assertThrows[Exception] {
      new StoreManager(dataDir, backupDir, columnFamilies, Some("nonExistingPath.ini"), ConfigDefaults.blockCacheSizeMB)
    }
  }

  it should "fail if specified config file is invalid" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  stats_dump_period_sec=700")

    assertThrows[Exception] {
      new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath), ConfigDefaults.blockCacheSizeMB)
    }
  }


}
