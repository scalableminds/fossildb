/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.vkvstore

import com.google.protobuf.ByteString
import com.scalableminds.vkvstore.db.VersionedKeyValueStore
import com.scalableminds.vkvstore.proto.messages._
import com.scalableminds.vkvstore.proto.rpcs.StoreGrpc

import scala.concurrent.Future

class StoreGrpcImpl(stores: Map[String, VersionedKeyValueStore]) extends StoreGrpc.Store {
  override def get(req: GetRequest) = {
    println("received get request")
    try {
      val store = stores.get(req.collection).get
      val versionedKeyValuePair = store.get(req.key, req.version).get
      Future.successful(GetReply(true, ByteString.copyFrom(versionedKeyValuePair.value), versionedKeyValuePair.version))
    } catch {
      case e: Exception => Future.successful(GetReply(false, ByteString.EMPTY, 0))
    }
  }

  override def put(req: PutRequest) = {
    val store = stores.get(req.collection).get

    try {
      require(req.version >= 0, "Version numbers must be non-negative")
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

  override def getMultipleVersions(req: GetMultipleVersionsRequest) = {
    val store = stores.get(req.collection).get

    try {
      val values = store.getMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
      Future.successful(GetMultipleVersionsReply(true, values.map(ByteString.copyFrom(_))))
    } catch {
      case e: Exception => Future.successful(GetMultipleVersionsReply(false))
    }
  }

  override def getMultipleKeys(req: GetMultipleKeysRequest) = {
    val store = stores.get(req.collection).get

    try {
      val (keys, values) = store.getMultipleKeys(req.key, req.prefix, req.version)
      Future.successful(GetMultipleKeysReply(true, keys, values.map(ByteString.copyFrom(_))))
    } catch {
      case e: Exception => Future.successful(GetMultipleKeysReply(false))
    }
  }

}
