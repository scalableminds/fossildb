/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import java.io.File
import java.nio.file.Paths

import com.google.protobuf.ByteString
import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.fossildbapi._
import io.grpc.ManagedChannelBuilder
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.concurrent.ExecutionContext

class FossilDBSuite extends FlatSpec with BeforeAndAfterEach {
  val testTempDir = "testData"
  val dataDir = Paths.get(testTempDir, "data")
  val backupDir = Paths.get(testTempDir, "backup")

  val port = 21505
  var serverOpt: Option[FossilDBServer] = None
  val client = FossilDBGrpc.blockingStub(ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext(true).build)

  val collectionA = "collectionA"
  val collectionB = "collectionB"

  val testData1 = ByteString.copyFromUtf8("testData1")
  val testData2 = ByteString.copyFromUtf8("testData2")

  val aKey = "aKey"
  val anotherKey = "anotherKey"

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  override def beforeEach = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()

    val columnFamilies = List(collectionA, collectionB)

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies)

    serverOpt.map(_.stop())
    serverOpt = Some(new FossilDBServer(storeManager, port, ExecutionContext.global))
    serverOpt.map(_.start())
  }

  override def afterEach = {
    serverOpt.map(_.stop())
    deleteRecursively(new File(testTempDir))
  }



  "Get" should "return matching value after matching Put" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

  it should "return matching value after multiple versioned Puts" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 5, testData1))
    client.put(PutRequest(collectionA, aKey, 2, testData2))
    val reply = client.get(GetRequest(collectionA, aKey, Some(2)))
    assert(testData2 == reply.value)
  }

  it should "return value of closest older version" in {
    client.put(PutRequest(collectionA, aKey, 2, testData1))
    client.put(PutRequest(collectionA, aKey, 5, testData2))

    val reply = client.get(GetRequest(collectionA, aKey, Some(7)))
    assert(testData2 == reply.value)
  }

  it should "return success == false if called on empty db" in {
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "return success == false after Put with other key" in {
    client.put(PutRequest(collectionA, anotherKey, 0, testData1))
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "return success == false after Put with only newer version" in {
    client.put(PutRequest(collectionA, aKey, 5, testData1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(3)))
    assert(!reply.success)
  }

  "Backup" should "create non-empty backup directory" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.backup(BackupRequest())
    val dir = new File(backupDir.toString)
    assert(dir.exists)
    assert(dir.isDirectory)
    assert(dir.listFiles.length > 0)
  }

  "Restore" should "fail if there are no backups" in {
    val reply = client.restoreFromBackup(RestoreFromBackupRequest())
    assert(!reply.success)
  }

  "Restore" should "restore old state after backup" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.backup(BackupRequest())
    client.delete(DeleteRequest(collectionA, aKey, 0))
    client.restoreFromBackup(RestoreFromBackupRequest())
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

}
