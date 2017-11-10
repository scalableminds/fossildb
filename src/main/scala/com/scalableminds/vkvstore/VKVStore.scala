package com.scalableminds.vkvstore

import java.nio.file.Paths

import com.google.protobuf.ByteString
import com.scalableminds.vkvstore.db.{RocksDBManager, VersionedKeyValueStore}
import com.scalableminds.vkvstore.proto.messages._
import com.scalableminds.vkvstore.proto.rpcs.StoreGrpc
import com.scalableminds.vkvstore.proto.rpcs.StoreGrpc.StoreBlockingStub
import io.grpc.ManagedChannelBuilder

import scala.concurrent.ExecutionContext

object VKVStore {
  def main(args: Array[String]) = {

    val dataDir = Paths.get("data")
    val columnFamilies = List("skeletons", "skeletonUpdates", "volumes", "volumeData")

    val rocksDBMangaer = new RocksDBManager(dataDir, columnFamilies)

    val stores = columnFamilies.map { cf =>
      val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBMangaer.getStoreForColumnFamily(cf).get)
      (cf -> store)
    }.toMap

    val server = new StoreServer(stores, 8090, ExecutionContext.global)

    server.start()
    //runTestClient()
    server.blockUntilShutdown()
    //TODO: close rocksdb on shutdown?


    //TODO: val backupInProgress = new AtomicBoolean(false)
  }

  def runTestClient() = {
    val channel = ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext(true).build
    val blockingStub = StoreGrpc.blockingStub(channel)
    testGetPutDelete(blockingStub)
    testGetVersions(blockingStub)
  }

  def testGetPutDelete(blockingStub: StoreBlockingStub) = {
    val deleteReply: DeleteReply = blockingStub.delete(DeleteRequest(collection = "collection1", key = "aKey", version = 0))
    println("tried delete v0. reply: ", deleteReply)

    val deleteReply2: DeleteReply = blockingStub.delete(DeleteRequest(collection = "collection1", key = "aKey", version = 1))
    println("tried delete v1. reply: ", deleteReply2)

    val getReply: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = Some(0)))
    println("tried get v0. reply: ", getReply)

    val putReply: PutReply = blockingStub.put(PutRequest(collection = "collection1", key = "aKey", version = 0, value = ByteString.copyFromUtf8("aValue")))
    println("tried put v0. reply: ", putReply)

    val putReply2: PutReply = blockingStub.put(PutRequest(collection = "collection1", key = "aKey", version = 1, value = ByteString.copyFromUtf8("aValue")))
    println("tried put v1. reply: ", putReply2)

    val getReply3: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = Some(0)))
    println("tried get again v0. reply: ", getReply3)

    val getReply4: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = Some(1)))
    println("tried get again v1. reply: ", getReply4)

    val getReply5: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = Some(1)))
    println("tried get again v2. reply: ", getReply5)
  }

  def testGetVersions(blockingStub: StoreBlockingStub) = {
    val collection = "collection1"
    val key = "bKey"

    blockingStub.put(PutRequest(collection, key, 2, ByteString.copyFromUtf8("version2")))
    blockingStub.put(PutRequest(collection, key, 3, ByteString.copyFromUtf8("version3")))
    blockingStub.put(PutRequest(collection, key, 3, ByteString.copyFromUtf8("version3-updated")))
    blockingStub.put(PutRequest(collection, key, 4, ByteString.copyFromUtf8("version4")))
    blockingStub.put(PutRequest(collection, key, 6, ByteString.copyFromUtf8("version6")))

    val getVersionsReply = blockingStub.getMultipleVersions(GetMultipleVersionsRequest(collection, key))
    println("getVersions all: ", getVersionsReply)


    val getVersionsReply2 = blockingStub.getMultipleVersions(GetMultipleVersionsRequest(collection, key, Some(3), Some(4)))
    println("getVersions some: ", getVersionsReply2)
  }
}
