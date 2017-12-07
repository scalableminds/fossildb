/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import java.io.{PrintWriter, StringWriter}

import com.google.protobuf.ByteString
import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.fossildbapi._
import com.trueaccord.scalapb.GeneratedMessage
import com.typesafe.scalalogging.LazyLogging
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}

class FossilDBGrpcImpl(storeManager: StoreManager) extends FossilDBGrpc.FossilDB with LazyLogging {

  override def health(req: HealthRequest, obs: StreamObserver[HealthReply]) = withExceptionHandler(req, obs) {
    HealthReply()
  }

  override def get(req: GetRequest, obs: StreamObserver[GetReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    val versionedKeyValuePairOpt = store.get(req.key, req.version)
    versionedKeyValuePairOpt match {
      case Some(pair) => GetReply(ByteString.copyFrom(pair.value), pair.version)
      case None => {
        if (!(req.mayBeEmpty.getOrElse(false))) throw new StatusRuntimeException(Status.NOT_FOUND)
        throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("mayBeEmpty"))
      }
    }
  }

  override def put(req: PutRequest, obs: StreamObserver[PutReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    val version = req.version.getOrElse(store.get(req.key, None).map(_.version + 1).getOrElse(0L))
    require(version >= 0, "Version numbers must be non-negative")
    store.put(req.key, version, req.value.toByteArray)
    PutReply()
  }

  override def delete(req: DeleteRequest, obs: StreamObserver[DeleteReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    store.delete(req.key, req.version)
    DeleteReply()
  }

  override def getMultipleVersions(req: GetMultipleVersionsRequest, obs: StreamObserver[GetMultipleVersionsReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    val (values, versions) = store.getMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
    GetMultipleVersionsReply(values.map(ByteString.copyFrom(_)), versions)
  }

  override def getMultipleKeys(req: GetMultipleKeysRequest, obs: StreamObserver[GetMultipleKeysReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    val (keys, values) = store.getMultipleKeys(req.key, req.prefix, req.version)
    GetMultipleKeysReply(keys, values.map(ByteString.copyFrom(_)))
  }

  override def deleteMultipleVersions(req: DeleteMultipleVersionsRequest, obs: StreamObserver[DeleteMultipleVersionsReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    store.deleteMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
    DeleteMultipleVersionsReply()
  }

  override def listKeys(req: ListKeysRequest, obs: StreamObserver[ListKeysReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    val keys = store.listKeys(req.limit, req.startAfterKey)
    ListKeysReply(keys)
  }

  override def listVersions(req: ListVersionsRequest, obs: StreamObserver[ListVersionsReply]) = withExceptionHandler(req, obs) {
    val store = storeManager.getStore(req.collection)
    val versions = store.listVersions(req.key, req.limit, req.offset)
    ListVersionsReply(versions)
  }

  override def backup(req: BackupRequest, obs: StreamObserver[BackupReply]) = withExceptionHandler(req, obs) {
    val backupInfoOpt = storeManager.backup
    backupInfoOpt match {
      case Some(backupInfo) => BackupReply(backupInfo.id, backupInfo.timestamp, backupInfo.size)
      case _ => throw new Exception("Backup did not return valid BackupInfo")
    }
  }

  override def restoreFromBackup(req: RestoreFromBackupRequest, obs: StreamObserver[RestoreFromBackupReply]) = withExceptionHandler(req, obs) {
    storeManager.restoreFromBackup
    RestoreFromBackupReply()
  }

  private def withExceptionHandler [T, R <: GeneratedMessage](request: R, responseObserver: StreamObserver[T])(tryBlock: => T): Unit = {
    try {
      logger.debug("received " + requestToString(request))
      responseObserver.onNext(tryBlock)
      responseObserver.onCompleted()
    } catch {
      case e: StatusRuntimeException => {
        if ((e.getStatus.getDescription() == null) || (!e.getStatus.getDescription.contains("mayBeEmpty"))) {
          log(e, request)
        }
        responseObserver.onError(e)
      }
      case e: Exception => {
        log(e, request)
        responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage)))
      }
    }
  }

  private def log[R <: GeneratedMessage](e: Exception, request: R) = {
    logger.warn(getStackTraceAsString(e) + "\nrequest that caused this error: " + requestToString(request) + "\n")
  }

  private def requestToString[R <: GeneratedMessage](request: R) =
    request.getClass.getSimpleName + "(" + request.toString.replaceAll("\n"," ") + ")"

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString.dropRight(1)
  }
}
