package com.scalableminds.fossildb

import java.io.File
import java.nio.file.{Files, Paths}
import com.google.protobuf.ByteString
import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.fossildbapi._
import com.typesafe.scalalogging.LazyLogging
import io.grpc.health.v1._
import io.grpc.netty.NettyChannelBuilder
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.concurrent.ExecutionContext

class FossilDBSuite extends FlatSpec with BeforeAndAfterEach with TestHelpers with LazyLogging {
  private val testTempDir = "testData1"
  private val dataDir = Paths.get(testTempDir, "data")
  private val backupDir = Paths.get(testTempDir, "backup")

  private val port = 21505
  private var serverOpt: Option[FossilDBServer] = None
  private val channel = NettyChannelBuilder.forAddress("127.0.0.1", port).maxInboundMessageSize(Int.MaxValue).usePlaintext().build
  private val client = FossilDBGrpc.blockingStub(channel)
  private val healthClient = HealthGrpc.newBlockingStub(channel)
  private val collectionA = "collectionA"
  private val collectionB = "collectionB"

  private val testData1 = ByteString.copyFromUtf8("testData1")
  private val testData2 = ByteString.copyFromUtf8("testData2")
  private val testData3 = ByteString.copyFromUtf8("testData3")

  private val aKey = "aKey"
  private val anotherKey = "anotherKey"
  private val aThirdKey = "aThirdKey"

  private val chunkSize = 1024

  override def beforeEach: Unit = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()

    val columnFamilies = List(collectionA, collectionB)

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies, None, chunkSize)

    serverOpt.foreach(_.stop())
    serverOpt = Some(new FossilDBServer(storeManager, port, ExecutionContext.global))
    serverOpt.foreach(_.start())
  }

  override def afterEach: Unit = {
    serverOpt.foreach(_.stop())
    deleteRecursively(new File(testTempDir))
  }

  "Health" should "reply" in {
    val reply = client.health(HealthRequest())
    assert(reply.success)
  }

  "GRPC Standard Health Check" should "report SERVING" in {
    val reply = healthClient.check(HealthCheckRequest.getDefaultInstance)
    assert(reply.getStatus.toString == "SERVING")
  }

  "Put" should "overwrite old value" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(0), testData2))
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData2 == reply.value)
  }

  it should "fail on non-existent collection" in {
    val reply = client.put(PutRequest("nonExistentCollection", aKey, Some(0), testData1))
    assert(!reply.success)
  }

  it should "increment version if none is supplied" in {
    client.put(PutRequest(collectionA, aKey, Some(4), testData1))
    client.put(PutRequest(collectionA, aKey, None, testData1))
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(reply.actualVersion == 5)
  }

  it should "start at version 0 if none is supplied" in {
    client.put(PutRequest(collectionA, aKey, None, testData1))
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(reply.actualVersion == 0)
  }

  "Get" should "return matching value after matching Put" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

  it should "return matching value after multiple versioned Puts" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(5), testData1))
    client.put(PutRequest(collectionA, aKey, Some(2), testData2))
    val reply = client.get(GetRequest(collectionA, aKey, Some(2)))
    assert(testData2 == reply.value)
  }

  it should "return value of closest older version" in {
    client.put(PutRequest(collectionA, aKey, Some(2), testData1))
    client.put(PutRequest(collectionA, aKey, Some(5), testData2))

    val reply = client.get(GetRequest(collectionA, aKey, Some(7)))
    assert(testData2 == reply.value)
  }

  it should "fail if called on empty db" in {
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "fail after Put with other key" in {
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData1))
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "fail after Put with only newer version" in {
    client.put(PutRequest(collectionA, aKey, Some(5), testData1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(3)))
    assert(!reply.success)
  }

  "Delete" should "delete a value at specific version" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.delete(DeleteRequest(collectionA, aKey, 1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(1)))
    assert(testData1 == reply.value)
  }

  "ListKeys" should "list all keys of a collection" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, anotherKey, Some(4), testData2))
    client.put(PutRequest(collectionB, aThirdKey, Some(1), testData1))
    val reply = client.listKeys(ListKeysRequest(collectionA))
    assert(reply.keys.contains(aKey))
    assert(reply.keys.contains(anotherKey))
    assert(reply.keys.length == 2)
  }

  it should "support pagination with startAfterKey" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, anotherKey, Some(4), testData2))
    client.put(PutRequest(collectionB, aThirdKey, Some(1), testData1))
    val reply = client.listKeys(ListKeysRequest(collectionA, Some(1)))
    assert(reply.keys.length == 1)
    assert(reply.keys.contains(aKey))
    val reply2 = client.listKeys(ListKeysRequest(collectionA, Some(1), Some(reply.keys.last)))
    assert(reply2.keys.contains(anotherKey))
    assert(reply2.keys.length == 1)
  }

  it should "return all keys despite lexicographic similarity" in {
    client.put(PutRequest(collectionA, "abb/1/1-[1,1,1]", Some(1), testData1))
    client.put(PutRequest(collectionA, "abc/1/1481800838-[3600,2717,121]", Some(123), testData2))
    client.put(PutRequest(collectionA, "abc/1/1481800839-[3601,2717,121]", Some(123), testData3))
    client.put(PutRequest(collectionA, "abc/1/1481800839-[3601,2717,121]", Some(125), testData3))
    client.put(PutRequest(collectionA, "abc/1/1481800839-[3601,2717,121]", Some(128), testData3))
    client.put(PutRequest(collectionA, "abc/1/1481800846-[3602,2717,121]", Some(123), testData2))

    val reply = client.listKeys(ListKeysRequest(collectionA, None, Some("abb")))
    assert(reply.keys.length == 3)
  }

  "GetMultipleVersions" should "return all versions in decending order if called without limits" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aKey, Some(2), testData3))
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData1))
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
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aKey, Some(3), testData3))
    client.put(PutRequest(collectionA, aKey, Some(4), testData1))
    client.put(PutRequest(collectionA, aKey, Some(5), testData1))
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData1))

    val reply = client.getMultipleVersions(GetMultipleVersionsRequest(collectionA, aKey, Some(4), Some(2)))
    assert(reply.versions(0) == 4)
    assert(reply.versions(1) == 3)
    assert(reply.values(0) == testData1)
    assert(reply.values(1) == testData3)
    assert(reply.versions.length == 2)
    assert(reply.values.length == 2)
  }

  "GetMultipleKeys" should "return keys starting with initial one (no prefix)" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData2))
    client.put(PutRequest(collectionA, aThirdKey, Some(0), testData3))
    val reply = client.getMultipleKeys(GetMultipleKeysRequest(collectionA, aThirdKey))
    assert(reply.keys.length == 2)
    assert(reply.keys.contains(anotherKey))
    assert(reply.keys.contains(aThirdKey))
    assert(reply.values.length == 2)
    assert(reply.values.contains(testData2))
    assert(reply.values.contains(testData3))
    assert(reply.actualVersions.length == 2)
    assert(reply.actualVersions.contains(0))
  }

  it should "return keys of matching version (sorted alphabetically)" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aThirdKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, anotherKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aThirdKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aKey, Some(2), testData3))
    client.put(PutRequest(collectionA, anotherKey, Some(2), testData3))
    client.put(PutRequest(collectionA, aThirdKey, Some(2), testData3))
    val reply = client.getMultipleKeys(GetMultipleKeysRequest(collectionA, aKey, None, Some(1)))
    assert(reply.keys.length == 3)
    assert(reply.values.contains(testData2))
  }

  it should "return keys of matching version, matching prefix (sorted alphabetically)" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aThirdKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, anotherKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aThirdKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aKey, Some(2), testData3))
    client.put(PutRequest(collectionA, anotherKey, Some(2), testData3))
    client.put(PutRequest(collectionA, aThirdKey, Some(2), testData3))
    val reply = client.getMultipleKeys(GetMultipleKeysRequest(collectionA, aKey, Some("aK"), Some(1)))
    assert(reply.keys.length == 1)
    assert(reply.values.contains(testData2))
    assert(reply.actualVersions.contains(1))
  }

  it should "with limit return only the first n keys of matching version " in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.put(PutRequest(collectionA, anotherKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aThirdKey, Some(0), testData1))
    client.put(PutRequest(collectionA, aKey, Some(1), testData2))
    client.put(PutRequest(collectionA, anotherKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aThirdKey, Some(1), testData2))
    client.put(PutRequest(collectionA, aKey, Some(2), testData3))
    client.put(PutRequest(collectionA, anotherKey, Some(2), testData3))
    client.put(PutRequest(collectionA, aThirdKey, Some(2), testData3))
    val reply = client.getMultipleKeys(GetMultipleKeysRequest(collectionA, aKey, None, Some(1), Some(2)))
    assert(reply.keys.length == 2)
    assert(reply.values.contains(testData2))
    assert(reply.actualVersions.contains(1))
  }

  "Backup" should "create non-empty backup directory" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
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
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.backup(BackupRequest())
    client.delete(DeleteRequest(collectionA, aKey, 0))
    client.restoreFromBackup(RestoreFromBackupRequest())
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

  it should "restore even after deletion of data dir" in {
    client.put(PutRequest(collectionA, aKey, Some(0), testData1))
    client.backup(BackupRequest())
    deleteRecursively(new File(dataDir.toString))
    client.restoreFromBackup(RestoreFromBackupRequest())
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

}
