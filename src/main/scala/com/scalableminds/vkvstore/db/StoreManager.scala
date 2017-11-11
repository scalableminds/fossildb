/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.vkvstore.db

import java.nio.file.Path

class StoreManager(dataDir: Path, columnFamilies: List[String]) {

  val rocksDBManager = new RocksDBManager(dataDir, columnFamilies)

  val stores = columnFamilies.map { cf =>
    val store: VersionedKeyValueStore = new VersionedKeyValueStore(rocksDBManager.getStoreForColumnFamily(cf).get)
    (cf -> store)
  }.toMap

  def getStore(columnFamily: String) = {
    stores.get(columnFamily).get
  }
}
