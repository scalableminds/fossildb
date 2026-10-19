package com.scalableminds.fossildb.db

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{BufferHasAsJava, ListHasAsScala, SeqHasAsJava}
import scala.language.postfixOps

case class BackupInfo(id: Int, timestamp: Long, size: Long)

case class KeyValuePair[T](key: String, value: T)

class RocksDBManager(dataDir: Path, columnFamilies: List[String], optionsFilePathOpt: Option[String]) extends LazyLogging {

  private val (db: RocksDB, columnFamilyHandles) = {
    RocksDB.loadLibrary()
    val columnOptions = new ColumnFamilyOptions()
      .setArenaBlockSize(4L * 1024 * 1024) // 4MB
      .setTargetFileSizeBase(1024L * 1024 * 1024) // 1GB
      .setMaxBytesForLevelBase(10L * 1024 * 1024 * 1024) // 10GB
    val options = new DBOptions()
    val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()
    optionsFilePathOpt.foreach { optionsFilePath =>
      try {
        val configOptions = new ConfigOptions()
        org.rocksdb.OptionsUtil.loadOptionsFromFile(configOptions, optionsFilePath, options, cfListRef.asJava)
        logger.info("successfully loaded rocksdb options from " + optionsFilePath)
      } catch {
        case e: Exception =>
          throw new Exception("Failed to load rocksdb options from file " + optionsFilePath, e)
      }
    }
    options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
    val defaultColumnFamilyName = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8)
    def nameOf(descriptor: ColumnFamilyDescriptor) = new String(descriptor.getName, StandardCharsets.UTF_8)
    val defaultColumnFamilyOptions: ColumnFamilyOptions = cfListRef.find(nameOf(_) == defaultColumnFamilyName).map(_.getOptions).getOrElse(columnOptions)
    // Compare by name (not by Array[Byte], which has reference equality), so column families from the options file are not added twice
    val columnFamiliesFromOptionsFile = cfListRef.map(nameOf).toSet
    val newColumnFamilyDescriptors = (columnFamilies :+ defaultColumnFamilyName).distinct.filterNot(columnFamiliesFromOptionsFile.contains)
      .map(name => new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), defaultColumnFamilyOptions))
    val columnFamilyDescriptors = cfListRef.toList ::: newColumnFamilyDescriptors
    logger.info("Opening RocksDB at " + dataDir.toAbsolutePath)
    val columnFamilyHandles = new util.ArrayList[ColumnFamilyHandle]
    val db = RocksDB.open(
      options,
      dataDir.toAbsolutePath.toString,
      columnFamilyDescriptors.asJava,
      columnFamilyHandles)
    // Handles are returned in descriptor order, which differs from the configured order when an options file is used
    val handlesByName = columnFamilyDescriptors.map(nameOf).zip(columnFamilyHandles.asScala).toMap
    (db, columnFamilies.map(name => name -> handlesByName(name)).toMap)
  }

  def getStoreForColumnFamily(columnFamily: String): Option[RocksDBStore] = {
    columnFamilyHandles.get(columnFamily).map(new RocksDBStore(db, _))
  }

  def backup(backupDir: Path): Option[BackupInfo] = {
    if (!Files.exists(backupDir) || !Files.isDirectory(backupDir))
      Files.createDirectories(backupDir)

    RocksDB.loadLibrary()
    val backupEngine = BackupEngine.open(Env.getDefault, new BackupEngineOptions(backupDir.toString))
    backupEngine.createNewBackup(db)
    backupEngine.purgeOldBackups(1)
    backupEngine.getBackupInfo.asScala.headOption.map(info => BackupInfo(info.backupId, info.timestamp, info.size))
  }

  def restoreFromBackup(backupDir: Path): Unit = {
    logger.info("Restoring from backup. RocksDB temporarily unavailable")
    close()
    RocksDB.loadLibrary()
    val backupEngine = BackupEngine.open(Env.getDefault, new BackupEngineOptions(backupDir.toString))
    backupEngine.restoreDbFromLatestBackup(dataDir.toString, dataDir.toString, new RestoreOptions(true))
    logger.info("Restoring from backup complete. Reopening RocksDB")
  }

  def compactAllData(): Unit = {
    logger.info("Compacting all data")
    RocksDB.loadLibrary()
    db.compactRange()
    logger.info("All data has been compacted to last level containing data")
  }

  def exportToNewDB(newDataDir: Path, newOptionsFilePathOpt: Option[String]): Unit = {
    RocksDB.loadLibrary()
    logger.info(s"Exporting to new DB at ${newDataDir.toString} with options file $newOptionsFilePathOpt")
    val newManager = new RocksDBManager(newDataDir, columnFamilies, newOptionsFilePathOpt)
    newManager.columnFamilyHandles.foreach { case (name, handle) =>
      val store = getStoreForColumnFamily(name).get
      store.withRawRocksIterator { rocksIt =>
        val dataIterator = RocksDBStore.scan(rocksIt, "", None)
        dataIterator.foreach(el => newManager.db.put(handle, el.key.getBytes, el.value))
      }
    }
    logger.info("Writing data completed. Start compaction")
    newManager.db.compactRange()
    logger.info("Compaction finished")
  }

  def close(): Future[Unit] = {
    logger.info("Closing RocksDB handle")
    Future.successful(db.close())
  }
}

class RocksDBKeyIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[String] with LazyLogging {

  override def hasNext: Boolean = it.isValid && prefix.forall(it.key().startsWith(_))

  override def next(): String = {
    val key = new String(it.key().map(_.toChar))
    it.next()
    key
  }

  def peek: String = {
    new String(it.key().map(_.toChar))
  }

}

class RocksDBIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[KeyValuePair[Array[Byte]]] {

  override def hasNext: Boolean = it.isValid && prefix.forall(it.key().startsWith(_))

  override def next(): KeyValuePair[Array[Byte]] = {
    val value = KeyValuePair(new String(it.key().map(_.toChar)), it.value())
    it.next()
    value
  }

}

class RocksDBStore(db: RocksDB, handle: ColumnFamilyHandle) extends LazyLogging {

  def withRawRocksIterator[T](block: RocksIterator => T): T = {
    val rocksIt = db.newIterator(handle)
    try {
      block(rocksIt)
    } finally {
      rocksIt.close()
    }
  }

  def get(key: String): Array[Byte] = {
    db.get(handle, key.getBytes())
  }

  def put(key: String, value: Array[Byte]): Unit = {
    db.put(handle, key.getBytes(), value)
  }

  def delete(key: String): Unit = {
    db.delete(handle, key.getBytes())
  }

}

object RocksDBStore {

  def scan(rocksIt: RocksIterator, key: String, prefix: Option[String]): RocksDBIterator = {
    rocksIt.seek(key.getBytes())
    new RocksDBIterator(rocksIt, prefix)
  }

  def scanKeysOnly(rocksIt: RocksIterator, key: String, prefix: Option[String]): RocksDBKeyIterator = {
    rocksIt.seek(key.getBytes())
    new RocksDBKeyIterator(rocksIt, prefix)
  }

}
