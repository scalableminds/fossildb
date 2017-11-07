package com.scalableminds.kvservice

import com.scalableminds.kvservice.proto.messages.{HelloReply, HelloRequest}
import com.scalableminds.kvservice.proto.rpcs.GreeterGrpc

import scala.concurrent.Future

class GreeterImpl extends GreeterGrpc.Greeter {
  override def sayHello(req: HelloRequest) = {
    val reply = HelloReply(message = "Hello " + req.name)
    Future.successful(reply)
  }
}
