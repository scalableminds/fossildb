/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.vkvstore

import java.io.{PrintWriter, StringWriter}

import com.google.protobuf.ByteString
import com.scalableminds.vkvstore.db.StoreManager
import com.scalableminds.vkvstore.proto.messages._
import com.scalableminds.vkvstore.proto.rpcs.StoreGrpc
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

class StoreGrpcImpl(storeManager: StoreManager) extends StoreGrpc.Store with LazyLogging {
  override def get(req: GetRequest) = {
    try {
      val store = storeManager.getStore(req.collection)
      val versionedKeyValuePair = store.get(req.key, req.version).get
      Future.successful(GetReply(true, None, ByteString.copyFrom(versionedKeyValuePair.value), versionedKeyValuePair.version))
    } catch {
      case e: Exception => log(e); Future.successful(GetReply(false, Some(e.toString), ByteString.EMPTY, 0))
    }
  }

  override def put(req: PutRequest) = {
    try {
      val store = storeManager.getStore(req.collection)
      require(req.version >= 0, "Version numbers must be non-negative")
      store.put(req.key, req.version, req.value.toByteArray)
      Future.successful(PutReply(true))
    } catch {
      case e: Exception => log(e); Future.successful(PutReply(false, Some(e.toString)))
    }
  }

  override def delete(req: DeleteRequest) = {
    try {
      val store = storeManager.getStore(req.collection)
      store.delete(req.key, req.version)
      Future.successful(DeleteReply(true))
    } catch {
      case e: Exception => log(e); Future.successful(DeleteReply(false, Some(e.toString)))
    }
  }

  override def getMultipleVersions(req: GetMultipleVersionsRequest) = {
    try {
      val store = storeManager.getStore(req.collection)
      val values = store.getMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
      Future.successful(GetMultipleVersionsReply(true, None, values.map(ByteString.copyFrom(_))))
    } catch {
      case e: Exception => log(e); Future.successful(GetMultipleVersionsReply(false, Some(e.toString)))
    }
  }

  override def getMultipleKeys(req: GetMultipleKeysRequest) = {
    try {
      val store = storeManager.getStore(req.collection)
      val (keys, values) = store.getMultipleKeys(req.key, req.prefix, req.version)
      Future.successful(GetMultipleKeysReply(true, None, keys, values.map(ByteString.copyFrom(_))))
    } catch {
      case e: Exception => log(e); Future.successful(GetMultipleKeysReply(false, Some(e.toString)))
    }
  }

  override def backup(req: BackupRequest) = {
    if (storeManager.backupInProgress.compareAndSet(false, true)) {
      try {
        val backupInfoOpt = storeManager.backup
        backupInfoOpt match {
          case Some(backupInfo) => Future.successful(BackupReply(true, None, backupInfo.id, backupInfo.timestamp, backupInfo.size))
          case _ => throw new Exception("Backup did not return valid BackupInfo")
        }
      } catch {
        case e: Exception => log(e); Future.successful(BackupReply(false, Some("Could not do backup: " + e.toString), "", 0, 0))
      } finally {
        storeManager.backupInProgress.set(false)
      }
    } else {
      Future.successful(BackupReply(false, Some("Backup already in progress."), "", 0, 0))
    }
  }

  override def restoreFromBackup(req: RestoreFromBackupRequest) = ???

  private def log(e: Exception) = logger.error(getStackTraceAsString(e))

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}
