package com.scalableminds.kvservice

import com.scalableminds.kvservice.proto.messages.{HelloReply, HelloRequest}
import com.scalableminds.kvservice.proto.rpcs.GreeterGrpc
import io.grpc.ManagedChannelBuilder

import scala.concurrent.ExecutionContext

object Hi {
  def main(args: Array[String]) = {
    val server = new HelloWorldServer(8090, ExecutionContext.global)
    server.start()
    runTestClient()
    server.blockUntilShutdown()
  }

  def runTestClient() = {

    val channel = ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext(true).build
    val request = HelloRequest(name = "World")

    val blockingStub = GreeterGrpc.blockingStub(channel)
    val reply: HelloReply = blockingStub.sayHello(request)
    println("got reply", reply)
  }
}
