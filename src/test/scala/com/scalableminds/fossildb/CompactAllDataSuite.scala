package com.scalableminds.fossildb

import com.scalableminds.fossildb.db.{ColumnFamily, FileByFileCompaction, StoreManager, VersionedKey}
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, CompactionOptions, CompressionType, DBOptions, FlushOptions, RocksDB}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsScala, SeqHasAsJava}
import scala.util.{Random, Using}

class CompactAllDataSuite extends AnyFlatSpec with BeforeAndAfterEach with TestHelpers {

  private val testTempDir = "testDataCompactAllData"
  private val dataDir = Paths.get(testTempDir, "data")
  private val backupDir = Paths.get(testTempDir, "backup")

  private val collections = List("collectionA", "collectionB")

  override def beforeEach(): Unit = {
    deleteRecursively(new File(testTempDir))
    new File(testTempDir).mkdir()
  }

  override def afterEach(): Unit = {
    deleteRecursively(new File(testTempDir))
  }

  private case class SstFile(collection: String, level: Int, name: String, compression: String)

  // Options of older FossilDB versions (snappy everywhere). Auto compactions are disabled, so files stay where
  // the tests put them.
  private def snappyOptions = new ColumnFamilyOptions().setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setDisableAutoCompactions(true)

  private def currentOptions = new ColumnFamilyOptions().setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION).setDisableAutoCompactions(true)

  // Opens the database directly, with the given options for all column families. Passes the collection handles.
  private def withRawDB[T](columnOptions: => ColumnFamilyOptions)(block: (RocksDB, Seq[ColumnFamilyHandle]) => T): T = {
    RocksDB.loadLibrary()
    dataDir.toFile.mkdirs()
    Using.Manager { use =>
      val options = use(columnOptions)
      val descriptors = (RocksDB.DEFAULT_COLUMN_FAMILY +: collections.map(_.getBytes)).map(new ColumnFamilyDescriptor(_, options))
      val handles = new java.util.ArrayList[ColumnFamilyHandle]
      val db = use(RocksDB.open(use(new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)), dataDir.toString, descriptors.asJava, handles))
      handles.asScala.foreach(use(_))
      block(db, handles.asScala.toSeq.tail)
    }.get
  }

  private def sstFiles(db: RocksDB, handles: Seq[ColumnFamilyHandle]): Seq[SstFile] =
    collections.zip(handles).flatMap { case (collection, handle) =>
      val compressionByFile = db.getPropertiesOfAllTables(handle).asScala.map { case (path, properties) =>
        Paths.get(path).getFileName.toString -> properties.getCompressionName
      }
      db.getColumnFamilyMetaData(handle).levels().asScala.toSeq.flatMap { level =>
        level.files().asScala.map { file =>
          val name = Paths.get(file.fileName()).getFileName.toString
          SstFile(collection, level.level(), name, compressionByFile(name))
        }
      }
    }

  private def sstFiles(): Seq[SstFile] = withRawDB(currentOptions)(sstFiles)

  private def withStoreManager[T](block: StoreManager => T): T = {
    val storeManager = new StoreManager(dataDir, backupDir, collections, None, ConfigDefaults.blockCacheSizeMB)
    try block(storeManager) finally storeManager.close
  }

  private def key(i: Int): String = f"key$i%06d"

  private def value(i: Int, size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    new Random(i).nextBytes(bytes)
    bytes
  }

  // Writes the keys into one new SST file, which is then moved to the given level. Without a level, it stays in level 0.
  private def writeFile(db: RocksDB, handle: ColumnFamilyHandle, keys: Range, level: Option[Int], valueSize: Int = 1000): Unit = {
    keys.foreach(i => db.put(handle, VersionedKey(key(i), 0).toString.getBytes, value(i, valueSize)))
    Using.resource(new FlushOptions().setWaitForFlush(true))(db.flush(_, handle))
    level.foreach { level =>
      val levelZeroFiles = db.getColumnFamilyMetaData(handle).levels().asScala.find(_.level() == 0).get.files().asScala.map(_.fileName())
      Using.resource(new CompactionOptions())(db.compactFiles(_, handle, levelZeroFiles.toList.asJava, level, -1, null))
    }
  }

  private def assertAllKeysReadable(storeManager: StoreManager, keys: Range, valueSize: Int = 1000): Unit =
    collections.foreach { collection =>
      val store = storeManager.getStore(collection)
      store.withRawRocksIterator { it =>
        keys.foreach(i => assert(store.get(it, key(i), None).exists(_.value.sameElements(value(i, valueSize))), s"$collection ${key(i)}"))
      }
    }

  "CompactAllData" should "rewrite files with outdated compression in level 1 and below, each into its level" in {
    withRawDB(snappyOptions) { (db, handles) =>
      handles.foreach { handle =>
        writeFile(db, handle, 0 until 100, Some(6))
        writeFile(db, handle, 100 until 200, Some(6))
        writeFile(db, handle, 200 until 300, Some(5))
        writeFile(db, handle, 300 until 310, None)
      }
      assert(sstFiles(db, handles).map(f => (f.level, f.compression)).sorted ==
        Seq.fill(2)(Seq((0, "Snappy"), (5, "Snappy"), (6, "Snappy"), (6, "Snappy"))).flatten.sorted)
    }

    Using.resource(currentOptions) { options =>
      withRawDB(currentOptions) { (db, handles) =>
        val columnFamilies = collections.zip(handles).map { case (c, h) => ColumnFamily(c, h, options) }
        new FileByFileCompaction(db, columnFamilies, 2, new AtomicBoolean(false), new ReentrantReadWriteLock()).run()
        val files = sstFiles(db, handles)
        // One output file per input file, in the same level. The bottommost level uses zstd, the others lz4.
        // Level-0 files are left to normal compaction.
        assert(files.map(f => (f.collection, f.level, f.compression)).sorted == collections.flatMap(c =>
          Seq((c, 0, "Snappy"), (c, 5, "LZ4"), (c, 6, "ZSTD"), (c, 6, "ZSTD"))).sorted)

        // Running it again does nothing
        new FileByFileCompaction(db, columnFamilies, 2, new AtomicBoolean(false), new ReentrantReadWriteLock()).run()
        assert(sstFiles(db, handles) == files)
      }
    }

    withStoreManager(assertAllKeysReadable(_, 0 until 310))
  }

  it should "convert all bottommost files of all collections with several threads while data is written" in {
    val fileCount = 10
    withRawDB(snappyOptions) { (db, handles) =>
      handles.foreach(handle => (0 until fileCount).foreach(i => writeFile(db, handle, i * 100 until (i + 1) * 100, Some(6))))
    }
    val storeManager = new StoreManager(dataDir, backupDir, collections, None, ConfigDefaults.blockCacheSizeMB, compactionThreads = 3)
    try {
      val writer = new Thread(() => (0 until 2000).foreach { i =>
        collections.foreach(collection => storeManager.getStore(collection).put(key(100000 + i), 0, value(100000 + i, 1000)))
      })
      writer.start()
      storeManager.compactAllData()
      writer.join()
      assertAllKeysReadable(storeManager, 0 until fileCount * 100)
      assertAllKeysReadable(storeManager, 100000 until 102000)
    } finally {
      storeManager.close
    }
    val bottommostFiles = sstFiles().filter(_.level == 6)
    assert(bottommostFiles.map(_.collection).distinct.sorted == collections)
    assert(bottommostFiles.size >= 2 * fileCount)
    assert(bottommostFiles.forall(_.compression == "ZSTD"))
  }

  it should "stop when the database is closed, and continue when called again" in {
    val fileCount = 10
    val valueSize = 40000 // 4 MB files, so closing likely happens during the compaction
    withRawDB(snappyOptions) { (db, handles) =>
      handles.foreach(handle => (0 until fileCount).foreach(i => writeFile(db, handle, i * 100 until (i + 1) * 100, Some(6), valueSize)))
    }
    val storeManager = new StoreManager(dataDir, backupDir, collections, None, ConfigDefaults.blockCacheSizeMB)
    val compaction = new Thread(() => try storeManager.compactAllData() catch {
      case _: Exception => () // Expected if closed in time
    })
    compaction.start()
    Thread.sleep(50)
    storeManager.close
    compaction.join(60000)
    assert(!compaction.isAlive)

    val files = sstFiles()
    assert(files.forall(f => f.level == 6 && Set("Snappy", "ZSTD").contains(f.compression)))

    withStoreManager { storeManager =>
      storeManager.compactAllData()
      assertAllKeysReadable(storeManager, 0 until fileCount * 100, valueSize)
    }
    assert(sstFiles().forall(_.compression == "ZSTD"))
  }

  "needsRewrite" should "accept the bottommost compression only in the bottommost level, and configured ones elsewhere" in {
    Using.resource(currentOptions) { options =>
      assert(!FileByFileCompaction.needsRewrite(options, "ZSTD", isBottommost = true))
      assert(FileByFileCompaction.needsRewrite(options, "LZ4", isBottommost = true))
      assert(FileByFileCompaction.needsRewrite(options, "Snappy", isBottommost = true))
      assert(!FileByFileCompaction.needsRewrite(options, "LZ4", isBottommost = false))
      assert(!FileByFileCompaction.needsRewrite(options, "ZSTD", isBottommost = false))
      assert(FileByFileCompaction.needsRewrite(options, "Snappy", isBottommost = false))
    }
    Using.resource(new ColumnFamilyOptions().setCompressionType(CompressionType.SNAPPY_COMPRESSION)) { options =>
      // Without a bottommost compression, the bottommost level uses the normal one
      assert(!FileByFileCompaction.needsRewrite(options, "Snappy", isBottommost = true))
      assert(FileByFileCompaction.needsRewrite(options, "NoCompression", isBottommost = true))
    }
  }
}
