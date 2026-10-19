package com.scalableminds.fossildb

import java.io.File
import java.nio.file.Paths
import com.scalableminds.fossildb.db.StoreManager
import org.rocksdb.{ColumnFamilyDescriptor, ConfigOptions, DBOptions, Env}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.jdk.CollectionConverters.BufferHasAsJava


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

  it should "map collections to the right column families when using a config file" in {
    val file = new File(testTempDir, "testConfig.ini")
    // lists collectionB before collectionA, and does not list every configured collection
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n\n[CFOptions \"default\"]\n\n[CFOptions \"collectionB\"]\n\n")

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath))
    def read(sm: StoreManager, collection: String) = {
      val store = sm.getStore(collection)
      store.withRawRocksIterator(it => store.get(it, "aKey", None)).map(pair => new String(pair.value))
    }
    storeManager.getStore(collectionA).put("aKey", 0, "valueA".getBytes)
    storeManager.getStore(collectionB).put("aKey", 0, "valueB".getBytes)
    assert(read(storeManager, collectionA).contains("valueA"))
    assert(read(storeManager, collectionB).contains("valueB"))
    storeManager.close

    // reopening without the config file must find the data in the same column families
    val reopenedStoreManager = new StoreManager(dataDir, backupDir, columnFamilies, None)
    assert(read(reopenedStoreManager, collectionA).contains("valueA"))
    assert(read(reopenedStoreManager, collectionB).contains("valueB"))
    reopenedStoreManager.close
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
