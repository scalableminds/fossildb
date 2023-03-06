package com.scalableminds.fossildb.db

import java.nio.file.{Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

class StoreManager(dataDir: Path, backupDir: Path, columnFamilies: List[String], rocksdbOptionsFile: Option[String], chunkSize: Int) {

  private var rocksDBManager: Option[RocksDBManager] = None
  private var stores: Option[Map[String, VersionedKeyValueStore]] = None

  reInitialize()

  private def reInitialize(): Unit = {
    rocksDBManager.map(_.close())
    rocksDBManager = Some(new RocksDBManager(dataDir, columnFamilies, rocksdbOptionsFile))
    stores = Some(columnFamilies.map { cf =>
      val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBManager.get.getStoreForColumnFamily(cf).get, chunkSize)
      cf -> store
    }.toMap)
  }

  def getStore(columnFamily: String): VersionedKeyValueStore = {
    failDuringRestore()
    try {
      val existingStores = stores.get
      existingStores(columnFamily)
    } catch {
      case _: Exception => throw new NoSuchElementException("No store for column family " + columnFamily)
    }
  }


  private val backupInProgress = new AtomicBoolean(false)
  private val restoreInProgress = new AtomicBoolean(false)

  private def failDuringRestore(): Unit = if (restoreInProgress.get) throw new Exception("Unavilable during restore-from-backup operation")
  private def failDuringBackup(): Unit = if (backupInProgress.get) throw new Exception("Unavilable during backup")


  def backup: Option[BackupInfo] = {
    failDuringRestore()
    if (backupInProgress.compareAndSet(false, true)) {
      try {
        rocksDBManager.get.backup(backupDir)
      } finally {
        backupInProgress.set(false)
      }
    } else {
      throw new Exception("Backup already in progress")
    }
  }

  def restoreFromBackup(): Unit = {
    failDuringBackup()
    if (restoreInProgress.compareAndSet(false, true)) {
      try {
        rocksDBManager.get.restoreFromBackup(backupDir)
      } finally {
        reInitialize()
        restoreInProgress.set(false)
      }
    } else {
      throw new Exception("Restore-from-backup already in progress")
    }
  }

  def compactAllData(): Unit = {
    failDuringBackup()
    failDuringRestore()
    rocksDBManager.get.compactAllData()
  }

  def exportDB(newDataDir: String, newOptionsFilePathOpt: Option[String]): Unit = {
    failDuringRestore()
    rocksDBManager.get.exportToNewDB(Paths.get(newDataDir), newOptionsFilePathOpt)
  }

  def close: Option[Future[Unit]] = {
    rocksDBManager.map(_.close())
  }
}
