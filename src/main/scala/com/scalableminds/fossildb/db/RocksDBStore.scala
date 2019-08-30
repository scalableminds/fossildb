/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb.db

import java.nio.file.{Files, Path}
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future

case class BackupInfo(id: Int, timestamp: Long, size: Long)

case class KeyValuePair[T](key: String, value: T)

class RocksDBManager(dataDir: Path, columnFamilies: List[String], optionsFilePathOpt: Option[String]) extends LazyLogging {

  val (db: RocksDB, columnFamilyHandles) = {
    RocksDB.loadLibrary()
    val options = new DBOptions()
    val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()
    optionsFilePathOpt.foreach { optionsFilePath =>
      try {
        org.rocksdb.OptionsUtil.loadOptionsFromFile(optionsFilePath, Env.getDefault, options, cfListRef.asJava)
        logger.info("successfully loaded rocksdb options from " + optionsFilePath)
      } catch {
        case e: Exception => {
          throw new Exception("Failed to load rocksdb options from file " + optionsFilePath, e)
        }
      }
    }
    options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
    val defaultColumnFamilyOptions = cfListRef.find(_.getName sameElements RocksDB.DEFAULT_COLUMN_FAMILY).map(_.getOptions).getOrElse(new ColumnFamilyOptions())
    val newColumnFamilyDescriptors = columnFamilies.map(_.getBytes).diff(cfListRef.toList.map(_.getName)).map(new ColumnFamilyDescriptor(_, defaultColumnFamilyOptions))
    val columnFamilyDescriptors = cfListRef.toList ::: newColumnFamilyDescriptors
    logger.info("Opening RocksDB at " + dataDir.toAbsolutePath)
    val columnFamilyHandles = new util.ArrayList[ColumnFamilyHandle]
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
    if (!Files.exists(backupDir) || !Files.isDirectory(backupDir))
      Files.createDirectories(backupDir)

    RocksDB.loadLibrary()
    val backupEngine = BackupEngine.open(Env.getDefault, new BackupableDBOptions(backupDir.toString))
    backupEngine.createNewBackup(db)
    backupEngine.purgeOldBackups(1)
    backupEngine.getBackupInfo.asScala.headOption.map(info => BackupInfo(info.backupId, info.timestamp, info.size))
  }

  def restoreFromBackup(backupDir: Path) = {
    logger.info("Restoring from backup. RocksDB temporarily unavailable")
    close()
    RocksDB.loadLibrary()
    val backupEngine = BackupEngine.open(Env.getDefault, new BackupableDBOptions(backupDir.toString))
    backupEngine.restoreDbFromLatestBackup(dataDir.toString, dataDir.toString, new RestoreOptions(true))
    logger.info("Restoring from backup complete. Reopening RocksDB")
  }

  def close(): Future[Unit] = {
    logger.info("Closing RocksDB handle")
    Future.successful(db.close())
  }
}

class RocksDBKeyIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[String] {

  override def hasNext: Boolean = it.isValid && prefix.forall(it.key().startsWith(_))

  override def next: String = {
    val key = new String(it.key().map(_.toChar))
    it.next()
    key
  }

}

class RocksDBIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[KeyValuePair[Array[Byte]]] {

  override def hasNext: Boolean = it.isValid && prefix.forall(it.key().startsWith(_))

  override def next: KeyValuePair[Array[Byte]] = {
    val value = KeyValuePair(new String(it.key().map(_.toChar)), it.value())
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

  def scanKeysOnly(key: String, prefix: Option[String]): Iterator[String] = {
    val it = db.newIterator(handle)
    it.seek(key.getBytes())
    new RocksDBKeyIterator(it, prefix)
  }

  def put(key: String, value: Array[Byte]) = {
    db.put(handle, key.getBytes(), value)
  }

  def delete(key: String) = {
    db.delete(handle, key.getBytes())
  }

}

