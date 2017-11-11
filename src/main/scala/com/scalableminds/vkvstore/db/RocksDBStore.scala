/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.vkvstore.db

import java.nio.file.{Files, Path}
import java.util

import org.rocksdb._

import scala.collection.JavaConverters._
import scala.concurrent.Future

case class BackupInfo(id: String, timestamp: Long, size: Long)

case class KeyValuePair[T](key: String, value: T)

class RocksDBManager(dataDir: Path, columnFamilies: List[String]) {

  val (db, columnFamilyHandles) = {
    RocksDB.loadLibrary()
    val columnOptions = new ColumnFamilyOptions()
      .setArenaBlockSize(4 * 1024 * 1024)               // 4MB
      .setTargetFileSizeBase(1024 * 1024 * 1024)        // 1GB
      .setMaxBytesForLevelBase(10 * 1024 * 1024 * 1024) // 10GB
    val columnFamilyDescriptors = (columnFamilies.map(_.getBytes) :+ RocksDB.DEFAULT_COLUMN_FAMILY).map { columnFamily =>
      new ColumnFamilyDescriptor(columnFamily, columnOptions)
    }
    val columnFamilyHandles = new util.ArrayList[ColumnFamilyHandle]
    val options = new DBOptions()
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)
    val db = RocksDB.open(
      options,
      dataDir.toAbsolutePath.toString,
      columnFamilyDescriptors.asJava,
      columnFamilyHandles)
    (db, columnFamilies.zip(columnFamilyHandles.asScala).toMap)
  }

  def getStoreForColumnFamily(columnFamily: String): Option[RocksDBStore] = {
    columnFamilyHandles.get(columnFamily).map(new RocksDBStore(db, _))
  }

  def backup(backupDir: Path): Option[BackupInfo] = {
    def ensureDirectory(path: Path): Path = {
      if (!Files.exists(path) || !Files.isDirectory(path))
        Files.createDirectories(path)
      path
    }

    ensureDirectory(backupDir)
    RocksDB.loadLibrary()
    val backupEngine = BackupEngine.open(Env.getDefault, new BackupableDBOptions(backupDir.toString))
    backupEngine.createNewBackup(db)
    backupEngine.purgeOldBackups(1)
    backupEngine.getBackupInfo.asScala.headOption.map(info => BackupInfo(info.backupId.toString, info.timestamp, info.size))
  }

  def close(): Future[Unit] = {
    Future.successful(db.close())
  }
}


class RocksDBIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[KeyValuePair[Array[Byte]]] {

  override def hasNext: Boolean = it.isValid && prefix.forall(it.key().startsWith(_))

  override def next: KeyValuePair[Array[Byte]] = {
    val value = KeyValuePair(new String(it.key().map(_.toChar)) , it.value())
    it.next()
    value
  }
}

class RocksDBStore(db: RocksDB, handle: ColumnFamilyHandle) {

  def get(key: String): Array[Byte] = {
    db.get(handle, key.getBytes())
  }

  def scan(key: String, prefix: Option[String]): Iterator[KeyValuePair[Array[Byte]]] = {
    val it = db.newIterator(handle)
    it.seek(key.getBytes())
    new RocksDBIterator(it, prefix)
  }

  def put(key: String, value: Array[Byte]) = {
    db.put(handle, key.getBytes(), value)
  }

  def delete(key: String) = {
    db.delete(handle, key.getBytes())
  }

}

