/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.rpcs.StoreGrpc
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.ExecutionContext

class StoreServer(storeManager: StoreManager, port: Int, executionContext: ExecutionContext) extends LazyLogging
{ self =>
  private[this] var server: Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort(port).addService(StoreGrpc.bindService(new StoreGrpcImpl(storeManager), executionContext)).build.start
    logger.info("Server started, listening on " + port)
    sys.addShutdownHook {
      logger.info("Shutting down gRPC server since JVM is shutting down")
      self.stop()
      logger.info("Server shut down")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
      storeManager.close
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

}



