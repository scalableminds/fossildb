package com.scalableminds.kvservice

import java.nio.file.Paths

import com.google.protobuf.ByteString
import com.scalableminds.kvservice.db.{RocksDBManager, VersionedKeyValueStore}
import com.scalableminds.kvservice.proto.messages._
import com.scalableminds.kvservice.proto.rpcs.StoreGrpc
import io.grpc.ManagedChannelBuilder

import scala.concurrent.ExecutionContext

object KVService {
  def main(args: Array[String]) = {

    val dataDir = Paths.get("data")
    val columnFamilies = List("collection1", "collection2")

    val rocksDBMangaer = new RocksDBManager(dataDir, columnFamilies)

    val stores = columnFamilies.map { cf =>
      val skeletonStore: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBMangaer.getStoreForColumnFamily(cf).get)
      (cf -> skeletonStore)
    }.toMap

    val server = new KVServer(stores, 8090, ExecutionContext.global)

    server.start()
    runTestClient()
    // server.blockUntilShutdown()
  }

  def runTestClient() = {
    val channel = ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext(true).build
    val blockingStub = StoreGrpc.blockingStub(channel)


    val deleteReply: DeleteReply = blockingStub.delete(DeleteRequest(collection = "collection1", key = "aKey", version = 0))
    println("tried delete v0. reply: ", deleteReply)

    val deleteReply2: DeleteReply = blockingStub.delete(DeleteRequest(collection = "collection1", key = "aKey", version = 1))
    println("tried delete v1. reply: ", deleteReply2)

    val getReply: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = 0))
    println("tried get v0. reply: ", getReply)

    val putReply: PutReply = blockingStub.put(PutRequest(collection = "collection1", key = "aKey", version = 0, value = ByteString.copyFromUtf8("aValue")))
    println("tried put v0. reply: ", putReply)

    val putReply2: PutReply = blockingStub.put(PutRequest(collection = "collection1", key = "aKey", version = 1, value = ByteString.copyFromUtf8("aValue")))
    println("tried put v1. reply: ", putReply2)

    val getReply3: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = 0))
    println("tried get again v0. reply: ", getReply3)

    val getReply4: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = 1))
    println("tried get again v1. reply: ", getReply4)

    val getReply5: GetReply = blockingStub.get(GetRequest(collection = "collection1", key = "aKey", version = 1))
    println("tried get again v2. reply: ", getReply5)
  }
}
