/*
 * Copyright (C) 2011-2018 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import java.io.File
import java.nio.file.Paths

import com.scalableminds.fossildb.db.StoreManager
import org.rocksdb.{ColumnFamilyDescriptor, DBOptions, Env}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.io.Source

class RocksOptionsSuite extends FlatSpec with BeforeAndAfterEach with TestHelpers {
/*
  val testTempDir = "testData2"
  val dataDir = Paths.get(testTempDir, "data")
  val backupDir = Paths.get(testTempDir, "backup")

  val collectionA = "collectionA"
  val collectionB = "collectionB"

  val columnFamilies = List(collectionA, collectionB)



  override def beforeEach = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()
  }

  override def afterEach = {
    deleteRecursively(new File(testTempDir))
  }



  "Initializing the StoreManager" should "load and use a specified config file" in {
    val file = new File(testTempDir, "testConfig.ini")
    writeToFile(file, "[Version]\n  rocksdb_version=5.11.3\n  options_file_version=1.1\n\n[DBOptions]\n  stats_dump_period_sec=700\n\n[CFOptions \"default\"]\n\n")

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, Some(file.getPath))

    var options = new DBOptions()
      .setStatsDumpPeriodSec(100)
    var cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()
    // if successful, the rocksdb writes the loaded options to a file that can then be retreived with loadLatestOptions
    // we test that that one now includes the value 700 from the file above, rather than the 100 specified as a default
    org.rocksdb.OptionsUtil.loadLatestOptions(dataDir.toString, Env.getDefault, options, cfListRef.asJava)
    assert(options.statsDumpPeriodSec() == 700)
    storeManager.close
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


*/
}
