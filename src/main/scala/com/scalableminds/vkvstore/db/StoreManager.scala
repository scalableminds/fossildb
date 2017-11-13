/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.vkvstore.db

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

class StoreManager(dataDir: Path, backupDir: Path, columnFamilies: List[String]) {

  val rocksDBManager = new RocksDBManager(dataDir, columnFamilies)

  val stores = columnFamilies.map { cf =>
    val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBManager.getStoreForColumnFamily(cf).get)
    (cf -> store)
  }.toMap

  def getStore(columnFamily: String) = {
    stores.get(columnFamily).get
  }

  val backupInProgress = new AtomicBoolean(false)

  def backup = rocksDBManager.backup(backupDir)
}
