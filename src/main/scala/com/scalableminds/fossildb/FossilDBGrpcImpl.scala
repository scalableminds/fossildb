/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb

import java.io.{PrintWriter, StringWriter}

import com.google.protobuf.ByteString
import io.grpc.health.v1.HealthGrpc
import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.fossildbapi._
import com.trueaccord.scalapb.GeneratedMessage
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

class FossilDBGrpcImpl(storeManager: StoreManager)
  extends FossilDBGrpc.FossilDB
  with LazyLogging
  with HealthGrpc.HealthImplBase {

  override def health(req: HealthRequest) = withExceptionHandler(req) {
    HealthReply(true)
  } {errorMsg => HealthReply(false, errorMsg)}

  override def get(req: GetRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val versionedKeyValuePairOpt = store.get(req.key, req.version)
    versionedKeyValuePairOpt match {
      case Some(pair) => GetReply(true, None, ByteString.copyFrom(pair.value), pair.version)
      case None => {
        if (!(req.mayBeEmpty.getOrElse(false))) throw new NoSuchElementException
        GetReply(false, Some("No such element"), ByteString.EMPTY, 0)
      }
    }
  } {errorMsg => GetReply(false, errorMsg, ByteString.EMPTY, 0)}

  override def put(req: PutRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val version = req.version.getOrElse(store.get(req.key, None).map(_.version + 1).getOrElse(0L))
    require(version >= 0, "Version numbers must be non-negative")
    store.put(req.key, version, req.value.toByteArray)
    PutReply(true)
  } {errorMsg => PutReply(false, errorMsg)}

  override def delete(req: DeleteRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    store.delete(req.key, req.version)
    DeleteReply(true)
  } {errorMsg => DeleteReply(false, errorMsg)}

  override def getMultipleVersions(req: GetMultipleVersionsRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val (values, versions) = store.getMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
    GetMultipleVersionsReply(true, None, values.map(ByteString.copyFrom(_)), versions)
  } {errorMsg => GetMultipleVersionsReply(false, errorMsg)}

  override def getMultipleKeys(req: GetMultipleKeysRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val (keys, values) = store.getMultipleKeys(req.key, req.prefix, req.version, req.limit)
    GetMultipleKeysReply(true, None, keys, values.map(ByteString.copyFrom(_)))
  } {errorMsg => GetMultipleKeysReply(false, errorMsg)}

  override def deleteMultipleVersions(req: DeleteMultipleVersionsRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    store.deleteMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
    DeleteMultipleVersionsReply(true)
  } {errorMsg => DeleteMultipleVersionsReply(false, errorMsg)}

  override def listKeys(req: ListKeysRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val keys = store.listKeys(req.limit, req.startAfterKey)
    ListKeysReply(true, None, keys)
  } {errorMsg => ListKeysReply(false, errorMsg)}

  override def listVersions(req: ListVersionsRequest) = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val versions = store.listVersions(req.key, req.limit, req.offset)
    ListVersionsReply(true, None, versions)
  } {errorMsg => ListVersionsReply(false, errorMsg)}

  override def backup(req: BackupRequest) = withExceptionHandler(req) {
    val backupInfoOpt = storeManager.backup
    backupInfoOpt match {
      case Some(backupInfo) => BackupReply(true, None, backupInfo.id, backupInfo.timestamp, backupInfo.size)
      case _ => throw new Exception("Backup did not return valid BackupInfo")
    }
  } {errorMsg => BackupReply(false, errorMsg, 0, 0, 0)}

  override def restoreFromBackup(req: RestoreFromBackupRequest) = withExceptionHandler(req) {
    storeManager.restoreFromBackup
    RestoreFromBackupReply(true)
  } {errorMsg => RestoreFromBackupReply(false, errorMsg)}


  private def withExceptionHandler [T, R <: GeneratedMessage](request: R)(tryBlock: => T)(onErrorBlock: Option[String] => T): Future[T] = {
    try {
      logger.debug("received " + requestToString(request))
      Future.successful(tryBlock)
    } catch {
      case e: Exception => {
        log(e, request)
        Future.successful(onErrorBlock(Some(e.toString)))
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
