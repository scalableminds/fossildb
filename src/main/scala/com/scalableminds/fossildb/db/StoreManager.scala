/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb.db

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

class StoreManager(dataDir: Path, backupDir: Path, columnFamilies: List[String]) {

  val rocksDBManager = new RocksDBManager(dataDir, columnFamilies)

  val stores = columnFamilies.map { cf =>
    val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBManager.getStoreForColumnFamily(cf).get)
    (cf -> store)
  }.toMap

  def getStore(columnFamily: String) = {
    try {
      stores.get(columnFamily).get
    } catch {
      case e: Exception => throw new NoSuchElementException("No store for column family " + columnFamily)
    }
  }

  val backupInProgress = new AtomicBoolean(false)

  def backup = rocksDBManager.backup(backupDir)
  def restoreFromBackup = rocksDBManager.restoreFromBackup(backupDir)

  def shutdown = rocksDBManager.close
}
