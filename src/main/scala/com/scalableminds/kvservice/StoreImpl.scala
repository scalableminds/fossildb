/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.kvservice

import com.google.protobuf.ByteString
import com.scalableminds.kvservice.db.RocksDBStore
import com.scalableminds.kvservice.proto.messages._
import com.scalableminds.kvservice.proto.rpcs.StoreGrpc

import scala.concurrent.Future

class StoreImpl(stores: Map[String, RocksDBStore]) extends StoreGrpc.Store {
  override def get(req: GetRequest) = {
    try {
      val store = stores.get(req.collection).get
      val value = store.get(req.key)
      Future.successful(GetReply(true, ByteString.copyFrom(value)))
    } catch {
      case e: Exception => Future.successful(GetReply(false, ByteString.EMPTY))
    }
  }

  override def put(req: PutRequest) = {
    val store = stores.get(req.collection).get

    try {
      store.put(req.key, req.value.toByteArray)
      Future.successful(PutReply(true))
    } catch {
      case e: Exception => Future.successful(PutReply(false))
    }
  }

  override def delete(req: DeleteRequest) = {
    val store = stores.get(req.collection).get

    try {
      store.delete(req.key)
      Future.successful(DeleteReply(true))
    } catch {
      case e: Exception => Future.successful(DeleteReply(false))
    }
  }

}
