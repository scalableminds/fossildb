package com.scalableminds.fossildb

import java.io.{PrintWriter, StringWriter}

import com.google.protobuf.ByteString
import com.scalableminds.fossildb.db.StoreManager
import com.scalableminds.fossildb.proto.fossildbapi._
import scalapb.GeneratedMessage
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

class FossilDBGrpcImpl(storeManager: StoreManager)
  extends FossilDBGrpc.FossilDB
    with LazyLogging {

  override def health(req: HealthRequest): Future[HealthReply] = withExceptionHandler(req) {
    HealthReply(success = true)
  } { errorMsg => HealthReply(success = false, errorMsg) }

  override def get(req: GetRequest): Future[GetReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val versionedKeyValuePairOpt = store.get(req.key, req.version)
    versionedKeyValuePairOpt match {
      case Some(pair) => GetReply(success = true, None, ByteString.copyFrom(pair.value), pair.version)
      case None =>
        if (!req.mayBeEmpty.getOrElse(false)) throw new NoSuchElementException
        GetReply(success = false, Some("No such element"), ByteString.EMPTY, 0)
    }
  } { errorMsg => GetReply(success = false, errorMsg, ByteString.EMPTY, 0) }

  override def put(req: PutRequest): Future[PutReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val version = req.version.getOrElse(store.get(req.key, None).map(_.version + 1).getOrElse(0L))
    require(version >= 0, "Version numbers must be non-negative")
    store.put(req.key, version, req.value.toByteArray)
    PutReply(success = true)
  } { errorMsg => PutReply(success = false, errorMsg) }

  override def delete(req: DeleteRequest): Future[DeleteReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    store.delete(req.key, req.version)
    DeleteReply(success = true)
  } { errorMsg => DeleteReply(success = false, errorMsg) }

  override def getMultipleVersions(req: GetMultipleVersionsRequest): Future[GetMultipleVersionsReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val (values, versions) = store.getMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
    GetMultipleVersionsReply(success = true, None, values.map(ByteString.copyFrom), versions)
  } { errorMsg => GetMultipleVersionsReply(success = false, errorMsg) }

  override def getMultipleKeys(req: GetMultipleKeysRequest): Future[GetMultipleKeysReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val (keys, values, versions) = store.getMultipleKeys(req.key, req.prefix, req.version, req.limit)
    GetMultipleKeysReply(success = true, None, keys, values.map(ByteString.copyFrom), versions)
  } { errorMsg => GetMultipleKeysReply(success = false, errorMsg) }

  override def deleteMultipleVersions(req: DeleteMultipleVersionsRequest): Future[DeleteMultipleVersionsReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    store.deleteMultipleVersions(req.key, req.oldestVersion, req.newestVersion)
    DeleteMultipleVersionsReply(success = true)
  } { errorMsg => DeleteMultipleVersionsReply(success = false, errorMsg) }

  override def listKeys(req: ListKeysRequest): Future[ListKeysReply] = withExceptionHandler(req) {
    val store = storeManager.getStore(req.collection)
    val keys = store.listKeys(req.limit, req.startAfterKey)
    ListKeysReply(success = true, None, keys)
  } { errorMsg => ListKeysReply(success = false, errorMsg) }

  override def listVersions(req: ListVersionsRequest): Future[ListVersionsReply] = withExceptionHandler(req) {
    val before = System.currentTimeMillis()
    val store = storeManager.getStore(req.collection)
    store.getUnderlying.seekAndMeasureTime(req.key)
    //val versions = store.listVersions(req.key, req.limit, req.offset)
    val versions = List(1L)
    val after = System.currentTimeMillis()
    logger.info(s"answering list versions took ${after - before} ms total")
    // store.getUnderlying.listAllKeys()
    ListVersionsReply(success = true, None, versions)
  } { errorMsg => ListVersionsReply(success = false, errorMsg) }

  override def backup(req: BackupRequest): Future[BackupReply] = withExceptionHandler(req) {
    val backupInfoOpt = storeManager.backup
    backupInfoOpt match {
      case Some(backupInfo) => BackupReply(success = true, None, backupInfo.id, backupInfo.timestamp, backupInfo.size)
      case _ => throw new Exception("Backup did not return valid BackupInfo")
    }
  } { errorMsg => BackupReply(success = false, errorMsg, 0, 0, 0) }

  override def restoreFromBackup(req: RestoreFromBackupRequest): Future[RestoreFromBackupReply] = withExceptionHandler(req) {
    storeManager.restoreFromBackup()
    RestoreFromBackupReply(success = true)
  } { errorMsg => RestoreFromBackupReply(success = false, errorMsg) }

  override def compactAllData(req: CompactAllDataRequest): Future[CompactAllDataReply] = withExceptionHandler(req) {
    storeManager.compactAllData()
    CompactAllDataReply(success = true)
  } { errorMsg => CompactAllDataReply(success = false, errorMsg) }

  override def exportDB(req: ExportDBRequest): Future[ExportDBReply] = withExceptionHandler(req) {
    storeManager.exportDB(req.newDataDir, req.optionsFile)
    ExportDBReply(success = true)
  } { errorMsg => ExportDBReply(success = false, errorMsg) }

  private def withExceptionHandler[T, R <: GeneratedMessage](request: R)(tryBlock: => T)(onErrorBlock: Option[String] => T): Future[T] = {
    try {
      logger.debug("received " + requestToString(request))
      Future.successful(tryBlock)
    } catch {
      case e: Exception =>
        log(e, request)
        Future.successful(onErrorBlock(Some(e.toString)))
    }
  }

  private def log[R <: GeneratedMessage](e: Exception, request: R): Unit = {
    logger.warn(getStackTraceAsString(e) + "\nrequest that caused this error: " + requestToString(request) + "\n")
  }

  private def requestToString[R <: GeneratedMessage](request: R) =
    request.getClass.getSimpleName + "(" + request.toString.replaceAll("\n", " ") + ")"

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString.dropRight(1)
  }
}
