package com.scalableminds.fossildb

import java.io.File
import java.nio.file.{Files, Paths}
import com.scalableminds.fossildb.db.StoreManager
import org.rocksdb.{ColumnFamilyDescriptor, ConfigOptions, DBOptions, Env}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.jdk.CollectionConverters.{BufferHasAsJava, IteratorHasAsScala}


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

  it should "use the shared block cache for index and filter blocks without a config file" in {
    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None, blockCacheSizeBytes = 64L * 1024 * 1024)
    val store = storeManager.getStore(collectionA)
    store.put("aKey", 0, Array[Byte](1))
    storeManager.compactAllData()
    assert(store.withRawRocksIterator(it => store.get(it, "aKey", None)).exists(_.value.sameElements(Array[Byte](1))))
    storeManager.close

    val optionsFileContent = new String(Files.readAllBytes(Files.list(dataDir).iterator().asScala.filter(_.getFileName.toString.startsWith("OPTIONS-")).max))
    assert(optionsFileContent.contains("cache_index_and_filter_blocks=true"))
    assert(optionsFileContent.contains("pin_l0_filter_and_index_blocks_in_cache=true"))
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
