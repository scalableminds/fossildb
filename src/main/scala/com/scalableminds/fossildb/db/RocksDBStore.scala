package com.scalableminds.fossildb.db

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters.{BufferHasAsJava, ListHasAsScala, SeqHasAsJava}
import scala.language.postfixOps
import scala.util.Using

case class BackupInfo(id: Int, timestamp: Long, size: Long)

case class KeyValuePair[T](key: String, value: T)

/**
 * @param maxBackgroundJobs RocksDB's max_background_jobs. If None, the value from the options file applies, or
 *                          RocksDBManager.defaultMaxBackgroundJobs without an options file.
 * @param compactionThreads number of files that compactAllData rewrites in parallel
 */
class RocksDBManager(dataDir: Path, columnFamilies: List[String], optionsFilePathOpt: Option[String],
                     blockCacheSizeMB: Long, maxBackgroundJobs: Option[Int] = None,
                     compactionThreads: Int = RocksDBManager.defaultCompactionThreads) extends AutoCloseable with LazyLogging {

  RocksDB.loadLibrary()

  // The native options objects must outlive the DB, they are released in close()
  // One block cache for data blocks, shared by all column families that use the built-in options. Column families
  // that get their options from an options file (including those using its "default" column family options) use the
  // table options from that file instead.
  // Index and filter blocks are not kept in this cache (RocksDB default), they are held in memory by each open SST file.
  private val blockCache = new LRUCache(blockCacheSizeMB * 1000 * 1000)
  private val tableConfig = new BlockBasedTableConfig().setBlockCache(blockCache)
  private val columnOptions = new ColumnFamilyOptions()
    .setArenaBlockSize(4L * 1024 * 1024) // 4MB
    .setTargetFileSizeBase(1024L * 1024 * 1024) // 1GB
    .setMaxBytesForLevelBase(10L * 1024 * 1024 * 1024) // 10GB
    // The bottommost level holds most of the data (including old versions), so compress it well with zstd.
    // Upper levels are rewritten frequently by compactions, so use the faster lz4 there.
    // Every SST file records its own compression, so files written with other compression types stay readable.
    // They are only rewritten when a compaction picks them up. compactAllData rewrites all of them.
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
    .setTableFormatConfig(tableConfig)
  private val options = new DBOptions()
  private val cfListRef: mutable.Buffer[ColumnFamilyDescriptor] = mutable.Buffer()

  // Set once close() starts. compactAllData holds closeLock for reading while it uses the DB, close() waits for it.
  private val closing = new AtomicBoolean(false)
  private val closeLock = new ReentrantReadWriteLock()

  private val (db: RocksDB, columnFamilyHandles, columnFamilyByCollection) = try {
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
    // RocksDB's default of 2 background jobs leaves a single compaction slot for the whole database (the other one is
    // for flushes). Then a long compaction, e.g. of the bottommost level, blocks all others, level 0 piles up and
    // RocksDB stalls writes. RocksDB only runs compactions in parallel when they fall behind, so more slots
    // don't cost anything otherwise.
    maxBackgroundJobs.orElse(if (optionsFilePathOpt.isEmpty) Some(RocksDBManager.defaultMaxBackgroundJobs) else None)
      .foreach(options.setMaxBackgroundJobs)
    logger.info(s"Using max_background_jobs=${options.maxBackgroundJobs()}")
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
    // Collections are mapped to handles by position, and handles come back in descriptor order: options file
    // column families first, then all configured ones (the diff above compares Array[Byte] by reference, so it
    // removes nothing). With an options file, collections therefore don't use the column family of the same name.
    // Existing databases rely on this mapping, so it must not change. See README.md#rocksdb-options-file.
    val columnFamiliesOfCollections = columnFamilies.zip(columnFamilyDescriptors.zip(columnFamilyHandles.asScala)).map {
      case (collection, (descriptor, handle)) => collection -> ColumnFamily(collection, handle, descriptor.getOptions)
    }.toMap
    (db, columnFamiliesOfCollections.view.mapValues(_.handle).toMap, columnFamiliesOfCollections)
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

  // Number of compactions RocksDB runs at most at the same time (GetBGJobLimits in RocksDB)
  @nowarn("cat=deprecation")
  private def compactionSlots: Int =
    if (options.maxBackgroundCompactions() == -1 && options.maxBackgroundFlushes() == -1)
      math.max(1, options.maxBackgroundJobs() - math.max(1, options.maxBackgroundJobs() / 4))
    else math.max(1, options.maxBackgroundCompactions())

  // Rewrites all files in level 1 and below that don't have the current compression, see FileByFileCompaction
  def compactAllData(): Unit = {
    // Each file rewrite takes a compaction slot. Leave at least one to automatic compactions, otherwise level 0
    // piles up and RocksDB stalls writes.
    val threads = math.max(1, math.min(compactionThreads, compactionSlots - 1))
    if (compactionSlots - threads < 1)
      logger.warn(s"There is only $compactionSlots compaction slot (max_background_jobs=${options.maxBackgroundJobs()}). " +
        "Automatic compactions can't run while CompactAllData rewrites a file, which may stall writes. Increase --maxBackgroundJobs.")
    else if (threads < compactionThreads)
      logger.warn(s"Using $threads instead of $compactionThreads compaction threads, to leave a compaction slot to " +
        s"automatic compactions ($compactionSlots slots with max_background_jobs=${options.maxBackgroundJobs()})")
    logger.info("Compacting all data, rewriting files with outdated compression one by one")
    new FileByFileCompaction(db, columnFamilyByCollection.values.toSeq.sortBy(cf => columnFamilies.indexOf(cf.collection)),
      threads, closing, closeLock).run()
    logger.info("Compacting all data done")
  }

  // Compacts everything into the bottommost level in one go. Only for databases without other users, as it
  // takes compaction slots for a long time.
  private def compactRangeAllColumnFamilies(): Unit =
    // A manual compaction skips the bottommost level by default. Forcing it rewrites all files with the
    // current options (e.g. compression), and drops deleted and overwritten entries there.
    Using.resource(new CompactRangeOptions().setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForceOptimized)) { compactRangeOptions =>
      // The data lives in the named column families, so compact each of them
      columnFamilyHandles.values.foreach(handle => db.compactRange(handle, null, null, compactRangeOptions))
    }

  def exportToNewDB(newDataDir: Path, newOptionsFilePathOpt: Option[String]): Unit = {
    logger.info(s"Exporting to new DB at ${newDataDir.toString} with options file $newOptionsFilePathOpt")
    Using.resource(new RocksDBManager(newDataDir, columnFamilies, newOptionsFilePathOpt, blockCacheSizeMB, maxBackgroundJobs,
      compactionThreads)) { newManager =>
      newManager.columnFamilyHandles.foreach { case (name, handle) =>
        val store = getStoreForColumnFamily(name).get
        store.withRawRocksIterator { rocksIt =>
          val dataIterator = RocksDBStore.scan(rocksIt, "", None)
          dataIterator.foreach(el => newManager.db.put(handle, RocksDBStore.encodeKey(el.key), el.value))
        }
      }
      // Nothing else uses the new database, so a single full compaction is fine. It moves all data to the
      // bottommost level, which a file-by-file compaction can't do for level 0.
      logger.info("Writing data completed. Start compaction")
      newManager.compactRangeAllColumnFamilies()
      logger.info("Compaction finished")
    }
  }

  override def close(): Unit = {
    logger.info("Closing RocksDB handle")
    if (closing.compareAndSet(false, true)) {
      // Aborts running compactions, including the CompactFiles calls of compactAllData
      db.cancelAllBackgroundWork(false)
    }
    // Wait until compactAllData doesn't use the DB anymore
    closeLock.writeLock().lock()
    try {
      // Closing is idempotent, so this is safe to call again, e.g. after restoreFromBackup
      db.close()
      closeOptions()
    } finally {
      closeLock.writeLock().unlock()
    }
  }

  private def closeOptions(): Unit = {
    (cfListRef.map(_.getOptions) :+ columnOptions).distinct.foreach(_.close())
    options.close()
    blockCache.close()
  }
}

object RocksDBManager {
  val defaultMaxBackgroundJobs: Int = 8
  val defaultCompactionThreads: Int = 2
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

  def withRawRocksIterator[T](block: RocksIterator => T): T = {
    val rocksIt = db.newIterator(handle)
    try {
      block(rocksIt)
    } finally {
      rocksIt.close()
    }
  }

  def get(key: String): Array[Byte] = {
    db.get(handle, RocksDBStore.encodeKey(key))
  }

  def put(key: String, value: Array[Byte]): Unit = {
    db.put(handle, RocksDBStore.encodeKey(key), value)
  }

  def delete(key: String): Unit = {
    db.delete(handle, RocksDBStore.encodeKey(key))
  }

  // Collects writes and applies them atomically once the block completes. If the block throws, nothing is written.
  // An empty batch is skipped, so requests that match nothing don't join a write group.
  def withWriteBatch[T](block: RocksDBWriteBatch => T): T =
    Using.resources(new WriteBatch(), new WriteOptions()) { (writeBatch, writeOptions) =>
      val result = block(new RocksDBWriteBatch(writeBatch, handle))
      if (writeBatch.count() > 0) db.write(writeOptions, writeBatch)
      result
    }

}

class RocksDBWriteBatch(writeBatch: WriteBatch, handle: ColumnFamilyHandle) {

  def put(key: String, value: Array[Byte]): Unit = {
    writeBatch.put(handle, RocksDBStore.encodeKey(key), value)
  }

  def delete(key: String): Unit = {
    writeBatch.delete(handle, RocksDBStore.encodeKey(key))
  }

}

object RocksDBStore {

  // Keys are always stored as UTF-8, independent of the platform's default charset
  def encodeKey(key: String): Array[Byte] = key.getBytes(StandardCharsets.UTF_8)

  def decodeKey(key: Array[Byte]): String = new String(key, StandardCharsets.UTF_8)

  def startsWith(key: Array[Byte], prefix: Array[Byte]): Boolean =
    key.length >= prefix.length && util.Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length)

  def scan(rocksIt: RocksIterator, key: String, prefix: Option[String]): RocksDBIterator = {
    rocksIt.seek(encodeKey(key))
    new RocksDBIterator(rocksIt, prefix)
  }

  def scanKeysOnly(rocksIt: RocksIterator, key: String, prefix: Option[String]): RocksDBKeyIterator = {
    rocksIt.seek(encodeKey(key))
    new RocksDBKeyIterator(rocksIt, prefix)
  }

}
