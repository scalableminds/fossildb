/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.kvservice

import com.scalableminds.kvservice.db.VersionedKeyValueStore
import com.scalableminds.kvservice.proto.rpcs.StoreGrpc
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.ExecutionContext

class KVServer(stores: Map[String, VersionedKeyValueStore], port: Int, executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort(port).addService(StoreGrpc.bindService(new StoreImpl(stores), executionContext)).build.start
    println("Server started, listening on " + port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
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



