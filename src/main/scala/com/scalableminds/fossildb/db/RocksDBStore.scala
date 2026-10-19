package com.scalableminds.fossildb.db

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.{BufferHasAsJava, ListHasAsScala, SeqHasAsJava}
import scala.language.postfixOps
import scala.util.Using

case class BackupInfo(id: Int, timestamp: Long, size: Long)

case class KeyValuePair[T](key: String, value: T)

class RocksDBManager(dataDir: Path, columnFamilies: List[String], optionsFilePathOpt: Option[String],
                     blockCacheSizeBytes: Long = RocksDBManager.defaultBlockCacheSizeBytes) extends AutoCloseable with LazyLogging {

  RocksDB.loadLibrary()

  // The native options objects must outlive the DB, they are released in close()
  // One block cache shared by all column families that use the default options (i.e. not configured in an options file)
  private val blockCache = new LRUCache(blockCacheSizeBytes)
  private val tableConfig = new BlockBasedTableConfig()
    .setBlockCache(blockCache)
    // Keep index and filter blocks in the block cache too, so their memory is bounded by the cache size.
    // Pinning the L0 ones avoids re-reading them for the newest (most frequently read) files.
    .setCacheIndexAndFilterBlocks(true)
    .setPinL0FilterAndIndexBlocksInCache(true)
  private val columnOptions = new ColumnFamilyOptions()
    .setArenaBlockSize(4L * 1024 * 1024) // 4MB
    .setTargetFileSizeBase(256L * 1024 * 1024) // 256MB
    .setMaxBytesForLevelBase(10L * 1024 * 1024 * 1024) // 10GB
    // The bottommost level holds most of the data (including old versions), so compress it well with zstd.
    // Upper levels are rewritten frequently by compactions, so use the faster lz4 there.
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
    .setTableFormatConfig(tableConfig)
  private val options = new DBOptions()
  private val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()

  private val (db: RocksDB, columnFamilyHandles) = try {
    optionsFilePathOpt.foreach { optionsFilePath =>
      try {
        Using.resource(new ConfigOptions()) { configOptions =>
          org.rocksdb.OptionsUtil.loadOptionsFromFile(configOptions, optionsFilePath, options, cfListRef.asJava)
        }
        logger.info("successfully loaded rocksdb options from " + optionsFilePath)
      } catch {
        case e: Exception =>
          throw new Exception("Failed to load rocksdb options from file " + optionsFilePath, e)
      }
    }
    options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
    val defaultColumnFamilyOptions: ColumnFamilyOptions = cfListRef.find(_.getName sameElements RocksDB.DEFAULT_COLUMN_FAMILY).map(_.getOptions).getOrElse(columnOptions)
    val newColumnFamilyDescriptors = (columnFamilies.map(_.getBytes) :+ RocksDB.DEFAULT_COLUMN_FAMILY).diff(cfListRef.toList.map(_.getName)).map(new ColumnFamilyDescriptor(_, defaultColumnFamilyOptions))
    val columnFamilyDescriptors = cfListRef.toList ::: newColumnFamilyDescriptors
    logger.info("Opening RocksDB at " + dataDir.toAbsolutePath)
    val columnFamilyHandles = new util.ArrayList[ColumnFamilyHandle]
    val db = RocksDB.open(
      options,
      dataDir.toAbsolutePath.toString,
      columnFamilyDescriptors.asJava,
      columnFamilyHandles)
    // The handles are returned in descriptor order, which differs from columnFamilies if an options file was loaded
    val handlesByName = columnFamilyDescriptors.map(descriptor => new String(descriptor.getName)).zip(columnFamilyHandles.asScala).toMap
    (db, columnFamilies.map(columnFamily => columnFamily -> handlesByName(columnFamily)).toMap)
  } catch {
    case e: Throwable =>
      // close() is unreachable if the constructor fails, so release the options here
      closeOptions()
      throw e
  }

  def getStoreForColumnFamily(columnFamily: String): Option[RocksDBStore] = {
    columnFamilyHandles.get(columnFamily).map(new RocksDBStore(db, _))
  }

  def backup(backupDir: Path): Option[BackupInfo] = {
    if (!Files.exists(backupDir) || !Files.isDirectory(backupDir))
      Files.createDirectories(backupDir)

    withBackupEngine(backupDir) { backupEngine =>
      backupEngine.createNewBackup(db)
      backupEngine.purgeOldBackups(1)
      backupEngine.getBackupInfo.asScala.headOption.map(info => BackupInfo(info.backupId, info.timestamp, info.size))
    }
  }

  def restoreFromBackup(backupDir: Path): Unit = {
    logger.info("Restoring from backup. RocksDB temporarily unavailable")
    close()
    withBackupEngine(backupDir) { backupEngine =>
      Using.resource(new RestoreOptions(true)) { restoreOptions =>
        backupEngine.restoreDbFromLatestBackup(dataDir.toString, dataDir.toString, restoreOptions)
      }
    }
    logger.info("Restoring from backup complete. Reopening RocksDB")
  }

  private def withBackupEngine[T](backupDir: Path)(block: BackupEngine => T): T =
    Using.resource(new BackupEngineOptions(backupDir.toString)) { backupEngineOptions =>
      Using.resource(BackupEngine.open(Env.getDefault, backupEngineOptions))(block)
    }

  def compactAllData(): Unit = {
    logger.info("Compacting all data")
    db.compactRange()
    logger.info("All data has been compacted to last level containing data")
  }

  def exportToNewDB(newDataDir: Path, newOptionsFilePathOpt: Option[String]): Unit = {
    logger.info(s"Exporting to new DB at ${newDataDir.toString} with options file $newOptionsFilePathOpt")
    // Closing releases the lock on the new DB, so it can be opened by another FossilDB instance
    Using.resource(new RocksDBManager(newDataDir, columnFamilies, newOptionsFilePathOpt, blockCacheSizeBytes)) { newManager =>
      newManager.columnFamilyHandles.foreach { case (name, handle) =>
        val store = getStoreForColumnFamily(name).get
        store.withRawRocksIterator { rocksIt =>
          val dataIterator = RocksDBStore.scan(rocksIt, "", None)
          dataIterator.foreach(el => newManager.db.put(handle, RocksDBStore.encodeKey(el.key), el.value))
        }
      }
      logger.info("Writing data completed. Start compaction")
      newManager.db.compactRange()
      logger.info("Compaction finished")
    }
  }

  override def close(): Unit = {
    logger.info("Closing RocksDB handle")
    // Closing is idempotent, so this is safe to call again, e.g. after restoreFromBackup
    db.close()
    closeOptions()
  }

  private def closeOptions(): Unit = {
    (cfListRef.map(_.getOptions) :+ columnOptions).distinct.foreach(_.close())
    options.close()
    blockCache.close()
  }
}

object RocksDBManager {
  val defaultBlockCacheSizeBytes: Long = 512L * 1024 * 1024 // 512MB
}

class RocksDBKeyIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[String] with LazyLogging {

  private val prefixBytes = prefix.map(RocksDBStore.encodeKey)

  override def hasNext: Boolean = it.isValid && prefixBytes.forall(RocksDBStore.startsWith(it.key(), _))

  override def next(): String = {
    val key = RocksDBStore.decodeKey(it.key())
    it.next()
    key
  }

  def peek: String = {
    RocksDBStore.decodeKey(it.key())
  }

}

class RocksDBIterator(it: RocksIterator, prefix: Option[String]) extends Iterator[KeyValuePair[Array[Byte]]] {

  private val prefixBytes = prefix.map(RocksDBStore.encodeKey)

  override def hasNext: Boolean = it.isValid && prefixBytes.forall(RocksDBStore.startsWith(it.key(), _))

  override def next(): KeyValuePair[Array[Byte]] = {
    val value = KeyValuePair(RocksDBStore.decodeKey(it.key()), it.value())
    it.next()
    value
  }

}

class RocksDBStore(db: RocksDB, handle: ColumnFamilyHandle) extends LazyLogging {

  def withRawRocksIterator[T](block: RocksIterator => T): T = withBoundedRocksIterator(None)(block)

  // With an upper bound (exclusive), the iterator becomes invalid at the bound instead of reading on into
  // the following keys, which is much cheaper when deletion tombstones follow the range of interest.
  def withBoundedRocksIterator[T](upperBound: Option[Array[Byte]])(block: RocksIterator => T): T =
    Using.Manager { use =>
      val readOptions = use(new ReadOptions())
      upperBound.foreach(bound => readOptions.setIterateUpperBound(use(new Slice(bound))))
      block(use(db.newIterator(handle, readOptions)))
    }.get

  def get(key: String): Array[Byte] = {
    db.get(handle, RocksDBStore.encodeKey(key))
  }

  def put(key: String, value: Array[Byte]): Unit = {
    db.put(handle, RocksDBStore.encodeKey(key), value)
  }

  def delete(key: String): Unit = {
    db.delete(handle, RocksDBStore.encodeKey(key))
  }

}

object RocksDBStore {

  // Keys are always stored as UTF-8, independent of the platform's default charset
  def encodeKey(key: String): Array[Byte] = key.getBytes(StandardCharsets.UTF_8)

  def decodeKey(key: Array[Byte]): String = new String(key, StandardCharsets.UTF_8)

  def startsWith(key: Array[Byte], prefix: Array[Byte]): Boolean =
    key.length >= prefix.length && util.Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length)

  // The smallest byte string greater than every key starting with prefix, None if there is none (empty or all 0xFF)
  def prefixUpperBound(prefix: Array[Byte]): Option[Array[Byte]] = {
    val lastIncrementableIndex = prefix.lastIndexWhere(_ != 0xFF.toByte)
    if (lastIncrementableIndex < 0) None
    else {
      val bound = prefix.take(lastIncrementableIndex + 1)
      bound(lastIncrementableIndex) = (bound(lastIncrementableIndex) + 1).toByte
      Some(bound)
    }
  }

  def scan(rocksIt: RocksIterator, key: String, prefix: Option[String]): RocksDBIterator = {
    rocksIt.seek(encodeKey(key))
    new RocksDBIterator(rocksIt, prefix)
  }

  def scanKeysOnly(rocksIt: RocksIterator, key: String, prefix: Option[String]): RocksDBKeyIterator = {
    rocksIt.seek(encodeKey(key))
    new RocksDBKeyIterator(rocksIt, prefix)
  }

}
