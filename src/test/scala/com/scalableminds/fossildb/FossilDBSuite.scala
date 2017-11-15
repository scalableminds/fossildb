/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import java.io.File
import java.nio.file.Paths

import com.google.protobuf.ByteString
import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.messages.{GetRequest, PutRequest}
import com.scalableminds.fossildb.proto.rpcs.StoreGrpc
import io.grpc.ManagedChannelBuilder
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.concurrent.ExecutionContext

class FossilDBSuite extends FlatSpec with BeforeAndAfterEach {
  val testDataDir = "testData"
  val port = 21505
  var serverOpt: Option[StoreServer] = None
  val client = StoreGrpc.blockingStub(ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext(true).build)

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
    deleteRecursively(new File(testDataDir))
    new File(testDataDir).mkdir()

    val dataDir = Paths.get(testDataDir, "data")
    val backupDir = Paths.get(testDataDir, "backup")
    val columnFamilies = List(collectionA, collectionB)

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies)

    serverOpt.map(_.stop())
    serverOpt = Some(new StoreServer(storeManager, port, ExecutionContext.global))
    serverOpt.map(_.start())
  }

  override def afterEach = {
    serverOpt.map(_.stop())
    deleteRecursively(new File(testDataDir))
  }



  "Get" should "return matching value after matching Put" in {
    client.put(PutRequest(collectionA, aKey, 0, testData1))
    val reply = client.get(GetRequest(collectionA, aKey, Some(0)))
    assert(testData1 == reply.value)
  }

  it should "return success == false if called on empty db" in {
    val reply = client.get(GetRequest(collectionA, aKey))
    assert(!reply.success)
  }

  it should "return success == false after put with other key" in {
    client.put(PutRequest(collectionA, anotherKey, 0, testData1))
    val reply = client.get(GetRequest(collectionA, aKey))
  }

}
