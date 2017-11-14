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
      logger.debug("received get: " + req.toString.replaceAll("\n"," "))
      val store = storeManager.getStore(req.collection)
      val versionedKeyValuePairOpt = store.get(req.key, req.version)
      versionedKeyValuePairOpt match {
        case Some(pair) => Future.successful(GetReply(true, None, ByteString.copyFrom(pair.value), pair.version))
        case None => {
          if (!(req.mayBeEmpty.getOrElse(false))) throw new NoSuchElementException
          Future.successful(GetReply(false, Some("No such element"), ByteString.EMPTY, 0))
        }
      }
    } catch {
      case e: Exception => {
        log(e, "get", req.toString)
        Future.successful(GetReply(false, Some(e.toString), ByteString.EMPTY, 0))
      }
    }
  }

  override def put(req: PutRequest) = {
    try {
      logger.debug("received put: " + req.toString.replaceAll("\n"," "))
      val store = storeManager.getStore(req.collection)
      require(req.version >= 0, "Version numbers must be non-negative")
      store.put(req.key, req.version, req.value.toByteArray)
      Future.successful(PutReply(true))
    } catch {
      case e: Exception => {
        log(e, "put", req.toString)
        Future.successful(PutReply(false, Some(e.toString)))
      }
    }
  }

  override def delete(req: DeleteRequest) = {
    try {
      logger.debug("received delete: " + req.toString.replaceAll("\n"," "))
      val store = storeManager.getStore(req.collection)
      store.delete(req.key, req.version)
      Future.successful(DeleteReply(true))
    } catch {
      case e: Exception => {
        log(e, "delete", req.toString)
        Future.successful(DeleteReply(false, Some(e.toString)))
      }
    }
  }

  override def getMultipleVersions(req: GetMultipleVersionsRequest) = {
    try {
      logger.debug("received getMultipleVersions: " + req.toString.replaceAll("\n"," "))
      val store = storeManager.getStore(req.collection)
      val values = store.getMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
      Future.successful(GetMultipleVersionsReply(true, None, values.map(ByteString.copyFrom(_))))
    } catch {
      case e: Exception => {
        log(e, "getMultipleVersions", req.toString)
        Future.successful(GetMultipleVersionsReply(false, Some(e.toString)))
      }
    }
  }

  override def getMultipleKeys(req: GetMultipleKeysRequest) = {
    try {
      logger.debug("received getMultipleKeys: " + req.toString.replaceAll("\n"," "))
      val store = storeManager.getStore(req.collection)
      val (keys, values) = store.getMultipleKeys(req.key, req.prefix, req.version)
      Future.successful(GetMultipleKeysReply(true, None, keys, values.map(ByteString.copyFrom(_))))
    } catch {
      case e: Exception => {
        log(e, "getMultipleKeys", req.toString)
        Future.successful(GetMultipleKeysReply(false, Some(e.toString)))
      }
    }
  }

  override def deleteMultipleVersions(req: DeleteMultipleVersionsRequest) = {
    try {
      logger.debug("received deleteMultipleVersions: " + req.toString.replaceAll("\n", " "))
      val store = storeManager.getStore(req.collection)
      store.deleteMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
      Future.successful(DeleteMultipleVersionsReply(true))
    } catch {
      case e: Exception => {
        log(e, "deleteMultipleVersions", req.toString)
        Future.successful(DeleteMultipleVersionsReply(false, Some(e.toString)))
      }
    }
  }

  override def listKeys(req: ListKeysRequest) = {
    try {
      logger.debug("received listKeys: " + req.toString.replaceAll("\n", " "))
      val store = storeManager.getStore(req.collection)
      val keys = store.listKeys(req.limit, req.offset)
      Future.successful(ListKeysReply(true, None, keys))
    } catch {
      case e: Exception => {
        log(e, "listKeys", req.toString)
        Future.successful(ListKeysReply(false, Some(e.toString)))
      }
    }
  }

  override def backup(req: BackupRequest) = {
    logger.debug("received backup: " + req.toString.replaceAll("\n"," "))
    if (storeManager.backupInProgress.compareAndSet(false, true)) {
      try {
        val backupInfoOpt = storeManager.backup
        backupInfoOpt match {
          case Some(backupInfo) => Future.successful(BackupReply(true, None, backupInfo.id, backupInfo.timestamp, backupInfo.size))
          case _ => throw new Exception("Backup did not return valid BackupInfo")
        }
      } catch {
        case e: Exception => {
          log(e, "backup", req.toString)
          Future.successful(BackupReply(false, Some("Could not do backup: " + e.toString), 0, 0, 0))
        }
      } finally {
        storeManager.backupInProgress.set(false)
      }
    } else {
      Future.successful(BackupReply(false, Some("Backup already in progress."), 0, 0, 0))
    }
  }

  override def restoreFromBackup(req: RestoreFromBackupRequest) = {
    try {
      logger.debug("received restoreFromBackup: " + req.toString.replaceAll("\n"," "))
      storeManager.restoreFromBackup
      Future.successful(RestoreFromBackupReply(true))
    } catch {
      case e: Exception => {
        log(e, "restoreFromBackup", req.toString)
        Future.successful(RestoreFromBackupReply(false, Some("Could not restore from backup: " + e.toString)))
      }
    }
  }

  private def log(e: Exception, requestType: String, requestFields: String) = {
    logger.error(getStackTraceAsString(e) + "\nrequest that caused this error: " + requestType + " (" + requestFields.replaceAll("\n"," ") + ")\n")
  }

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString.dropRight(1)
  }
}
