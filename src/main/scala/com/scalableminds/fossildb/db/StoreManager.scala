/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb.db

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging

class StoreManager(dataDir: Path, backupDir: Path, columnFamilies: List[String]) extends LazyLogging {

  var rocksDBManager: Option[RocksDBManager] = None
  var stores: Option[Map[String, VersionedKeyValueStore]] = None

  reInitialize

  def reInitialize = {
    rocksDBManager.map(_.close)
    rocksDBManager = Some(new RocksDBManager(dataDir, columnFamilies))
    stores = Some(columnFamilies.map { cf =>
      val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBManager.get.getStoreForColumnFamily(cf).get)
      (cf -> store)
    }.toMap)
  }

  def getStore(columnFamily: String) = {
    failDuringRestore
    try {
      stores.get.get(columnFamily).get
    } catch {
      case e: Exception => throw new NoSuchElementException("No store for column family " + columnFamily)
    }
  }


  val backupInProgress = new AtomicBoolean(false)
  val restoreInProgress = new AtomicBoolean(false)

  def failDuringRestore = if (restoreInProgress.get) throw new Exception("Unavilable during restore-from-backup operation")
  def failDuringBackup = if (backupInProgress.get) throw new Exception("Unavilable during backup")


  def backup = {
    failDuringRestore
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

  def restoreFromBackup = {
    failDuringBackup
    if (restoreInProgress.compareAndSet(false, true)) {
      try {
        rocksDBManager.get.restoreFromBackup(backupDir)
      } finally {
        reInitialize
        restoreInProgress.set(false)
      }
    } else {
      throw new Exception("Restore-from-backup already in progress")
    }
  }

  def close = {
    rocksDBManager.map(_.close)
  }


  def fixHexVersions = {
    val store: RocksDBStore = rocksDBManager.get.getStoreForColumnFamily("skeletons").get
    val keyIt = store.scanKeysOnly("", None)
    logger.info("starting to fix hex version keys")
    var count = 0
    while (keyIt.hasNext) {
      val oldKey = keyIt.next
      val oldKeyParts = oldKey.split('@')
      if (oldKeyParts(1).length != 16) {
        val value = store.get(oldKey)
        val newKey = oldKeyParts(0) + "@FFFFFFFFFFFFFFFF@" + oldKeyParts(2)
        store.put(newKey, value)
        store.delete(oldKey)
        count += 1
        if (count % 1000 == 0) logger.info("fixed " + count + " hex version keys")
        Thread.sleep(1)
      }
    }
    logger.info("done fixing hex version keys")
  }
}
