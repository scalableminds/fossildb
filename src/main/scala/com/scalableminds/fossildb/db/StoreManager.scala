package com.scalableminds.fossildb.db

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean

case class DatabaseInfo(dataDir: String, backupDir: String, rocksOptionsFile: Option[String], diskTotalBytes: Long,
                        diskUsableBytes: Long, backupInProgress: Boolean, compactionInProgress: Boolean,
                        rocksDBInfo: RocksDBInfo)

class StoreManager(dataDir: Path, backupDir: Path, columnFamilies: List[String], rocksdbOptionsFile: Option[String],
                   blockCacheSizeMB: Long) {

  private var rocksDBManager: Option[RocksDBManager] = None
  private var stores: Option[Map[String, VersionedKeyValueStore]] = None

  reInitialize()

  private def reInitialize(): Unit = {
    rocksDBManager.foreach(_.close())
    rocksDBManager = Some(new RocksDBManager(dataDir, columnFamilies, rocksdbOptionsFile, blockCacheSizeMB))
    stores = Some(columnFamilies.map { cf =>
      val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBManager.get.getStoreForColumnFamily(cf).get)
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
  private val compactionInProgress = new AtomicBoolean(false)

  private def failDuringRestore(): Unit = if (restoreInProgress.get) throw new Exception("Unavailable during restore-from-backup operation")
  private def failDuringBackup(): Unit = if (backupInProgress.get) throw new Exception("Unavailable during backup")
  private def failDuringCompaction(): Unit = if (compactionInProgress.get) throw new Exception("Unavailable during compaction")


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
    failDuringCompaction()
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
    if (compactionInProgress.compareAndSet(false, true)) {
      try {
        rocksDBManager.get.compactAllData()
      } finally {
        compactionInProgress.set(false)
      }
    } else {
      throw new Exception("Compaction already in progress")
    }
  }

  def exportDB(newDataDir: String, newOptionsFilePathOpt: Option[String]): Unit = {
    failDuringRestore()
    rocksDBManager.get.exportToNewDB(Paths.get(newDataDir), newOptionsFilePathOpt)
  }

  def databaseInfo: DatabaseInfo = {
    failDuringRestore()
    val fileStore = Files.getFileStore(dataDir)
    DatabaseInfo(
      dataDir = dataDir.toAbsolutePath.toString,
      backupDir = backupDir.toAbsolutePath.toString,
      rocksOptionsFile = rocksdbOptionsFile,
      diskTotalBytes = fileStore.getTotalSpace,
      diskUsableBytes = fileStore.getUsableSpace,
      backupInProgress = backupInProgress.get,
      compactionInProgress = compactionInProgress.get,
      rocksDBInfo = rocksDBManager.get.databaseInfo()
    )
  }

  def close: Unit = {
    rocksDBManager.foreach(_.close())
  }
}
