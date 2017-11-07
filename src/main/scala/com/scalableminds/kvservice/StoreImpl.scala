/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.kvservice

import com.google.protobuf.ByteString
import com.scalableminds.kvservice.db.RocksDBStore
import com.scalableminds.kvservice.proto.messages.{GetReply, GetRequest}
import com.scalableminds.kvservice.proto.rpcs.StoreGrpc

import scala.concurrent.Future

class StoreImpl(stores: List[(String, RocksDBStore)]) extends StoreGrpc.Store {
  override def get(req: GetRequest) = {
    val value = stores.head._2.get(req.key)

    val reply = GetReply(ByteString.copyFrom(value))
    Future.successful(reply)
  }
}
