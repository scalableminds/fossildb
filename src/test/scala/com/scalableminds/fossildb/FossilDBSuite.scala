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
  val collectionC = "collectionC"

  val testData1 = ByteString.copyFromUtf8("testData1")
  val testData2 = ByteString.copyFromUtf8("testData2")
  val testData3 = ByteString.copyFromUtf8("testData3")

  val aKey = "aKey"
  val anotherKey = "anotherKey"
  val aThirdKey = "aThirdKey"

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


  "Put" should "overwrite old value" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 0, testData2))
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData2 == reply.value)
  }

  it should "fail on non-existent collection" in {
    val reply = client.put(PutRequest("nonExistentCollection", aKey, 0, testData1))
    assert(!reply.success)
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

  it should "fail if called on empty db" in {
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "fail after Put with other key" in {
    client.put(PutRequest(collectionA, anotherKey, 0, testData1))
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "fail after Put with only newer version" in {
    client.put(PutRequest(collectionA, aKey, 5, testData1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(3)))
    assert(!reply.success)
  }

  "Delete" should "delete a value at specific version" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 1, testData2))
    client.delete(DeleteRequest(collectionA, aKey, 1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(1)))
    assert(testData1 == reply.value)
  }

  "ListKeys" should "list all keys of a collection" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 1, testData2))
    client.put(PutRequest(collectionA, anotherKey, 4, testData2))
    client.put(PutRequest(collectionB, aThirdKey, 1, testData1))
    val reply = client.listKeys(ListKeysRequest(collectionA))
    assert(reply.keys.contains(aKey))
    assert(reply.keys.contains(anotherKey))
    assert(reply.keys.length == 2)
  }

  "GetMultipleVersions" should "return all versions in decending order if called without limits" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 1, testData2))
    client.put(PutRequest(collectionA, aKey, 2, testData3))
    client.put(PutRequest(collectionA, anotherKey, 0, testData1))
    val reply = client.getMultipleVersions(GetMultipleVersionsRequest(collectionA, aKey))
    assert(reply.versions(0) == 2)
    assert(reply.versions(1) == 1)
    assert(reply.versions(2) == 0)
    assert(reply.values(0) == testData3)
    assert(reply.values(1) == testData2)
    assert(reply.values(2) == testData1)
    assert(reply.versions.length == 3)
    assert(reply.values.length == 3)
  }

  it should "return versions specified by bounds (inclusive)" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 1, testData2))
    client.put(PutRequest(collectionA, aKey, 3, testData3))
    client.put(PutRequest(collectionA, aKey, 4, testData1))
    client.put(PutRequest(collectionA, aKey, 5, testData1))
    client.put(PutRequest(collectionA, anotherKey, 0, testData1))

    val reply = client.getMultipleVersions(GetMultipleVersionsRequest(collectionA, aKey, Some(4), Some(2)))
    assert(reply.versions(0) == 4)
    assert(reply.versions(1) == 3)
    assert(reply.values(0) == testData1)
    assert(reply.values(1) == testData3)
    assert(reply.versions.length == 2)
    assert(reply.values.length == 2)
  }

  "GetMultipleKeys" should "return keys starting with initial one (no prefix)" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, anotherKey, 0, testData2))
    client.put(PutRequest(collectionA, aThirdKey, 0, testData3))
    val reply = client.getMultipleKeys(GetMultipleKeysRequest(collectionA, aThirdKey))
    assert(reply.keys.length == 2)
    assert(reply.keys.contains(anotherKey))
    assert(reply.keys.contains(aThirdKey))
    assert(reply.values.length == 2)
    assert(reply.values.contains(testData2))
    assert(reply.values.contains(testData3))
  }

  ignore should "return keys of matching version" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.put(PutRequest(collectionA, anotherKey, 0, testData1))
    client.put(PutRequest(collectionA, aThirdKey, 0, testData1))
    client.put(PutRequest(collectionA, aKey, 1, testData2))
    client.put(PutRequest(collectionA, anotherKey, 1, testData2))
    client.put(PutRequest(collectionA, aThirdKey, 1, testData2))
    client.put(PutRequest(collectionA, aKey, 2, testData3))
    client.put(PutRequest(collectionA, anotherKey, 2, testData3))
    client.put(PutRequest(collectionA, aThirdKey, 2, testData3))
    val reply = client.getMultipleKeys(GetMultipleKeysRequest(collectionA, aThirdKey, None, Some(2)))
    assert(reply.keys.length == 3)
    assert(reply.values.contains(testData1))
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

  it should "restore old state after backup" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.backup(BackupRequest())
    client.delete(DeleteRequest(collectionA, aKey, 0))
    client.restoreFromBackup(RestoreFromBackupRequest())
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

  it should "restore even after deletion of data dir" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    client.backup(BackupRequest())
    deleteRecursively(new File(dataDir.toString))
    client.restoreFromBackup(RestoreFromBackupRequest())
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

}
