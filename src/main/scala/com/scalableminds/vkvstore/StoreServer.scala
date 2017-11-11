/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.vkvstore

import com.scalableminds.vkvstore.db.{StoreManager, VersionedKeyValueStore}
import com.scalableminds.vkvstore.proto.rpcs.StoreGrpc
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.ExecutionContext

class StoreServer(storeManager: StoreManager, port: Int, executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort(port).addService(StoreGrpc.bindService(new StoreGrpcImpl(storeManager), executionContext)).build.start
    println("Server started, listening on " + port)
    sys.addShutdownHook {
      println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      println("*** server shut down")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

}



