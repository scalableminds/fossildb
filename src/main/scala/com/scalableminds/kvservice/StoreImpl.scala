/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.kvservice

import com.google.protobuf.ByteString
import com.scalableminds.kvservice.db.VersionedKeyValueStore
import com.scalableminds.kvservice.proto.messages._
import com.scalableminds.kvservice.proto.rpcs.StoreGrpc

import scala.concurrent.Future

class StoreImpl(stores: Map[String, VersionedKeyValueStore]) extends StoreGrpc.Store {
  override def get(req: GetRequest) = {
    try {
      val store = stores.get(req.collection).get
      val versionedKeyValuePair = store.get(req.key).get
      Future.successful(GetReply(true, ByteString.copyFrom(versionedKeyValuePair.value), versionedKeyValuePair.version))
    } catch {
      case e: Exception => Future.successful(GetReply(false, ByteString.EMPTY, 0))
    }
  }

  override def put(req: PutRequest) = {
    val store = stores.get(req.collection).get

    try {
      store.put(req.key, req.version, req.value.toByteArray)
      Future.successful(PutReply(true))
    } catch {
      case e: Exception => Future.successful(PutReply(false))
    }
  }

  override def delete(req: DeleteRequest) = {
    val store = stores.get(req.collection).get

    try {
      store.delete(req.key, req.version)
      Future.successful(DeleteReply(true))
    } catch {
      case e: Exception => Future.successful(DeleteReply(false))
    }
  }

}
