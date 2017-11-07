package com.scalableminds.kvservice

import java.nio.file.Paths

import com.scalableminds.kvservice.db.{RocksDBManager, RocksDBStore}
import com.scalableminds.kvservice.proto.messages.{GetReply, GetRequest}
import com.scalableminds.kvservice.proto.rpcs.StoreGrpc
import io.grpc.ManagedChannelBuilder

import scala.concurrent.ExecutionContext

object KVService {
  def main(args: Array[String]) = {

    val dataDir = Paths.get("data")
    val columnFamilies = List("skeletons", "skeletonUpdates", "volumes", "volumeData")

    val rocksDBMangaer = new RocksDBManager(dataDir, columnFamilies)

    val stores = columnFamilies.map { cf =>
      val skeletonStore: RocksDBStore = rocksDBMangaer.getStoreForColumnFamily(cf).get
      (cf, skeletonStore)
    }

    val server = new KVServer(stores, 8090, ExecutionContext.global)

    server.start()
    runTestClient()
    server.blockUntilShutdown()
  }

  def runTestClient() = {

    val channel = ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext(true).build
    val request = GetRequest(collection = "skeletons", key = "aKey", version = 0)

    val blockingStub = StoreGrpc.blockingStub(channel)
    val reply: GetReply = blockingStub.get(request)
    println("got reply", reply)
  }
}
