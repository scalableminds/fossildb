/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.fossildbapi.FossilDBGrpc
import io.grpc.health.v1.HealthGrpc
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.grpc.services.HealthStatusManager

import scala.concurrent.ExecutionContext

class FossilDBServer(storeManager: StoreManager, port: Int, executionContext: ExecutionContext) extends LazyLogging
{ self =>
  private[this] var server: Server = null
  private[this] var healthStatusManager: HealthStatusManager = null

  def start(): Unit = {
    healthStatusManager = new HealthStatusManager()
    server = NettyServerBuilder.forPort(port).maxMessageSize(Int.MaxValue)
      .addService(FossilDBGrpc.bindService(new FossilDBGrpcImpl(storeManager), executionContext))
      .addService(healthStatusManager.getHealthService())
      .build.start
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



