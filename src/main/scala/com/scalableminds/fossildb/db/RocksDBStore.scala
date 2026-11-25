package com.scalableminds.fossildb.db

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.{BufferHasAsJava, ListHasAsScala, MapHasAsScala, SeqHasAsJava}
import scala.language.postfixOps
import scala.util.{Try, Using}

case class BackupInfo(id: Int, timestamp: Long, size: Long)

case class KeyValuePair[T](key: String, value: T)

case class RocksDBInfo(walSizeBytes: Long, writesStopped: Boolean, delayedWriteRateBytesPerSecond: Long,
                       backgroundErrors: Long, runningCompactions: Long, runningFlushes: Long,
                       collections: List[CollectionInfo])

case class CollectionInfo(name: String, columnFamily: String, estimatedVersionCount: Long, sstFileCount: Long,
                          totalSstFilesBytes: Long, liveSstFilesBytes: Long, estimatedLiveDataBytes: Long,
                          sstEntries: Long, sstDeletions: Long, rawKeyBytes: Long, rawValueBytes: Long,
                          indexBytes: Long, filterBytes: Long, memtableBytes: Long, memtableEntries: Long,
                          memtableDeletions: Long, tableReadersMemoryBytes: Long, estimatedPendingCompactionBytes: Long,
                          compactionPending: Boolean, oldestSstFileCreationTimeSeconds: Option[Long],
                          newestSstFileCreationTimeSeconds: Option[Long], firstKey: Option[String],
                          lastKey: Option[String], blockCacheCapacityBytes: Option[Long],
                          blockCacheUsageBytes: Option[Long], blockCachePinnedUsageBytes: Option[Long],
                          levels: List[LevelInfo])

case class LevelInfo(level: Int, fileCount: Long, sizeBytes: Long, compressions: List[String])

class RocksDBManager(dataDir: Path, columnFamilies: List[String], optionsFilePathOpt: Option[String],
                     blockCacheSizeMB: Long) extends AutoCloseable with LazyLogging {

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
    // Collections are mapped to handles by position, and handles come back in descriptor order: options file
    // column families first, then all configured ones (the diff above compares Array[Byte] by reference, so it
    // removes nothing). With an options file, collections therefore don't use the column family of the same name.
    // Existing databases rely on this mapping, so it must not change. See README.md#rocksdb-options-file.
    (db, columnFamilies.zip(columnFamilyHandles.asScala).toMap)
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
    compactAllColumnFamilies()
    logger.info("All data has been compacted to last level containing data")
  }

  private def compactAllColumnFamilies(): Unit =
    // A manual compaction skips the bottommost level by default. Forcing it rewrites all files with the
    // current options (e.g. compression), and drops deleted and overwritten entries there.
    Using.resource(new CompactRangeOptions().setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForceOptimized)) { compactRangeOptions =>
      // The data lives in the named column families, so compact each of them
      columnFamilyHandles.values.foreach(handle => db.compactRange(handle, null, null, compactRangeOptions))
    }

  def exportToNewDB(newDataDir: Path, newOptionsFilePathOpt: Option[String]): Unit = {
    logger.info(s"Exporting to new DB at ${newDataDir.toString} with options file $newOptionsFilePathOpt")
    Using.resource(new RocksDBManager(newDataDir, columnFamilies, newOptionsFilePathOpt, blockCacheSizeMB)) { newManager =>
      newManager.columnFamilyHandles.foreach { case (name, handle) =>
        val store = getStoreForColumnFamily(name).get
        store.withRawRocksIterator { rocksIt =>
          val dataIterator = RocksDBStore.scan(rocksIt, "", None)
          dataIterator.foreach(el => newManager.db.put(handle, RocksDBStore.encodeKey(el.key), el.value))
        }
      }
      logger.info("Writing data completed. Start compaction")
      newManager.compactAllColumnFamilies()
      logger.info("Compaction finished")
    }
  }

  // Reads only metadata and counters that RocksDB keeps anyway, no data is scanned
  def databaseInfo(): RocksDBInfo = RocksDBInfo(
    walSizeBytes = db.getSortedWalFiles.asScala.map(_.sizeFileBytes).sum,
    // These properties are database-wide
    writesStopped = db.getLongProperty("rocksdb.is-write-stopped") != 0,
    delayedWriteRateBytesPerSecond = db.getLongProperty("rocksdb.actual-delayed-write-rate"),
    backgroundErrors = db.getLongProperty("rocksdb.background-errors"),
    runningCompactions = db.getLongProperty("rocksdb.num-running-compactions"),
    runningFlushes = db.getLongProperty("rocksdb.num-running-flushes"),
    collections = columnFamilies.map(name => collectionInfo(name, columnFamilyHandles(name)))
  )

  private def collectionInfo(name: String, handle: ColumnFamilyHandle): CollectionInfo = {
    def property(name: String): Long = db.getLongProperty(handle, name)
    // Fails if the column family has no block cache
    def blockCacheProperty(name: String): Option[Long] = Try(property(name)).toOption

    // Keyed by file name. Metadata and table properties are read separately, so a compaction in between can make
    // them disagree on which files exist. Files missing from the properties are then left out of the compressions.
    val tableProperties = db.getPropertiesOfAllTables(handle).asScala.map { case (path, properties) =>
      Paths.get(path).getFileName.toString -> properties
    }.toMap
    val metaData = db.getColumnFamilyMetaData(handle)
    val levels = metaData.levels.asScala.toList.filter(_.files.size > 0).map { level =>
      val compressions = level.files.asScala.flatMap(file =>
        tableProperties.get(Paths.get(file.fileName).getFileName.toString)).map(_.getCompressionName).distinct.sorted
      LevelInfo(level.level, level.files.size, level.size, compressions.toList)
    }
    val allProperties = tableProperties.values
    // 0 means unknown, e.g. for files written by old RocksDB versions
    val creationTimes = allProperties.map(_.getCreationTime).filter(_ > 0)

    val (firstKey, lastKey) = boundaryKeys(handle)

    CollectionInfo(
      name = name,
      columnFamily = new String(handle.getName, StandardCharsets.UTF_8),
      estimatedVersionCount = property("rocksdb.estimate-num-keys"),
      sstFileCount = metaData.fileCount,
      totalSstFilesBytes = property("rocksdb.total-sst-files-size"),
      liveSstFilesBytes = property("rocksdb.live-sst-files-size"),
      estimatedLiveDataBytes = property("rocksdb.estimate-live-data-size"),
      sstEntries = allProperties.map(_.getNumEntries).sum,
      sstDeletions = allProperties.map(_.getNumDeletions).sum,
      rawKeyBytes = allProperties.map(_.getRawKeySize).sum,
      rawValueBytes = allProperties.map(_.getRawValueSize).sum,
      indexBytes = allProperties.map(_.getIndexSize).sum,
      filterBytes = allProperties.map(_.getFilterSize).sum,
      memtableBytes = property("rocksdb.cur-size-all-mem-tables"),
      memtableEntries = property("rocksdb.num-entries-active-mem-table") + property("rocksdb.num-entries-imm-mem-tables"),
      memtableDeletions = property("rocksdb.num-deletes-active-mem-table") + property("rocksdb.num-deletes-imm-mem-tables"),
      tableReadersMemoryBytes = property("rocksdb.estimate-table-readers-mem"),
      estimatedPendingCompactionBytes = property("rocksdb.estimate-pending-compaction-bytes"),
      compactionPending = property("rocksdb.compaction-pending") != 0,
      oldestSstFileCreationTimeSeconds = creationTimes.minOption,
      newestSstFileCreationTimeSeconds = creationTimes.maxOption,
      firstKey = firstKey,
      lastKey = lastKey,
      blockCacheCapacityBytes = blockCacheProperty("rocksdb.block-cache-capacity"),
      blockCacheUsageBytes = blockCacheProperty("rocksdb.block-cache-usage"),
      blockCachePinnedUsageBytes = blockCacheProperty("rocksdb.block-cache-pinned-usage"),
      levels = levels
    )
  }

  // Deleted entries stay in the data as deletion markers until a compaction removes them, and the iterator has to
  // skip them. After a large DeleteAllByPrefix, that could take long, so give up after skipping too many.
  private val maxSkippedEntriesForBoundaryKeys = 100000L

  private def boundaryKeys(handle: ColumnFamilyHandle): (Option[String], Option[String]) =
    Using.Manager { use =>
      val readOptions = use(new ReadOptions().setMaxSkippableInternalKeys(maxSkippedEntriesForBoundaryKeys))
      val it = use(db.newIterator(handle, readOptions))
      // The iterator is invalid if the collection is empty or too many entries were skipped
      def currentKey: Option[String] =
        Option.when(it.isValid) {
          val compositeKey = RocksDBStore.decodeKey(it.key())
          VersionedKey(compositeKey).map(_.key).getOrElse(compositeKey)
        }
      it.seekToFirst()
      val first = currentKey
      it.seekToLast()
      (first, currentKey)
    }.get

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
