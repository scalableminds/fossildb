package com.scalableminds.fossildb.db

import com.scalableminds.fossildb.CloseableIterator
import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

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
    val defaultColumnFamilyOptions: ColumnFamilyOptions = cfListRef.find(_.getName sameElements RocksDB.DEFAULT_COLUMN_FAMILY).map(_.getOptions).getOrElse(columnOptions)
    println(defaultColumnFamilyOptions)
    val newColumnFamilyDescriptors = (columnFamilies.map(_.getBytes) :+ RocksDB.DEFAULT_COLUMN_FAMILY).diff(cfListRef.toList.map(_.getName)).map(new ColumnFamilyDescriptor(_, defaultColumnFamilyOptions))
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
      val rawIt = store.getRawIterator
      val dataIterator = RocksDBStore.scan(rawIt, "", None)
      dataIterator.foreach(el => newManager.db.put(handle, el.key.getBytes, el.value))
      println("close iterator")
      rawIt.close()
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

  def getRawIterator: RocksIterator = {
    println("creating iterator")
    db.newIterator(handle)
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

  def scan(it: RocksIterator, key: String, prefix: Option[String]): RocksDBIterator = {
    it.seek(key.getBytes())
    new RocksDBIterator(it, prefix)
  }

  def scanKeysOnly(it: RocksIterator, key: String, prefix: Option[String]): RocksDBKeyIterator = {
    it.seek(key.getBytes())
    new RocksDBKeyIterator(it, prefix)
  }

}