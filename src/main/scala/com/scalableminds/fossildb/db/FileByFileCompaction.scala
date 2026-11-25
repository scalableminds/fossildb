package com.scalableminds.fossildb.db

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

import java.nio.file.Paths
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.{DelayQueue, Delayed, Executors, ThreadFactory, TimeUnit}
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsScala, SeqHasAsJava}
import scala.util.Using

case class ColumnFamily(collection: String, handle: ColumnFamilyHandle, options: ColumnFamilyOptions)

/**
 * Rewrites the SST files whose compression differs from the current options, one file at a time, with CompactFiles.
 *
 * A full manual compaction (CompactRange) of a large column family runs in a single thread for hours and holds
 * compaction slots that automatic compactions need, so level-0 files pile up and RocksDB stalls writes.
 * Rewriting single files keeps each compaction short (about 15 s per 1 GB file) and leaves the remaining
 * compaction slots to automatic compactions, so writes continue at full speed meanwhile.
 *
 * Only files in level 1 and below are rewritten, each into its own level. A level-0 file spans almost the whole key
 * range, so it can only be compacted together with most of the level below. Normal compaction moves them down
 * once more data is written.
 *
 * Files that already have the configured compression are skipped, so running this again continues where an
 * interrupted run stopped, and does nothing once all files are converted.
 *
 * @param closing   set once the database is closing, then no further files are rewritten
 * @param closeLock held for reading while this uses the database, close() takes it for writing, so the database
 *                  isn't closed in between (calls on a closed RocksDB object crash the JVM)
 */
class FileByFileCompaction(db: RocksDB, columnFamilies: Seq[ColumnFamily], threads: Int,
                           closing: AtomicBoolean, closeLock: ReadWriteLock) extends LazyLogging {

  // New files may become outdated while a pass runs (e.g. a level-5 file trivially moved to the bottommost level
  // keeps its lz4 compression), so files are listed again after each pass. With constant writes, this could go on
  // forever, so stop after a few passes. Calling CompactAllData again rewrites the rest.
  private val maxPasses = 3
  private val busyRetryDelayMillis = 2000L

  private case class FileToRewrite(columnFamily: ColumnFamily, fileName: String, level: Int, size: Long,
                                   compression: String, order: Int) {
    override def toString: String = s"${columnFamily.collection} L$level $fileName (${size / 1000000} MB, $compression)"
  }

  def run(): Unit = {
    val start = System.nanoTime()
    var pass = 1
    var filesToRewrite = whileOpen(listFilesToRewrite())
    var rewrittenCount = 0
    while (filesToRewrite.nonEmpty && pass <= maxPasses) {
      rewrittenCount += rewrite(filesToRewrite, pass)
      pass += 1
      filesToRewrite = whileOpen(listFilesToRewrite())
    }
    if (filesToRewrite.nonEmpty)
      logger.warn(s"${filesToRewrite.size} files (${formatBytes(filesToRewrite.map(_.size).sum)}) were added with an " +
        s"outdated compression during compaction, they are left as they are. Run CompactAllData again to rewrite them.")
    logger.info(s"Rewrote $rewrittenCount files in ${formatDuration(System.nanoTime() - start)}")
  }

  private def listFilesToRewrite(): Seq[FileToRewrite] = {
    val files = columnFamilies.flatMap { cf =>
      val levels = db.getColumnFamilyMetaData(cf.handle).levels().asScala.filter(!_.files().isEmpty).toSeq
      val bottommostLevel = levels.map(_.level()).maxOption.getOrElse(0)
      val compressionByFile = db.getPropertiesOfAllTables(cf.handle).asScala.map { case (path, properties) =>
        baseName(path) -> properties.getCompressionName
      }
      for {
        level <- levels if level.level() > 0
        file <- level.files().asScala
        // A file that is missing here was just deleted by a compaction
        compression <- compressionByFile.get(baseName(file.fileName()))
        if needsRewrite(cf.options, compression, level.level() == bottommostLevel)
      } yield (cf, file, level.level(), compression)
    }
    files.zipWithIndex.map { case ((cf, file, level, compression), index) =>
      FileToRewrite(cf, file.fileName(), level, file.size(), compression, index)
    }
  }

  // Returns the number of rewritten files
  private def rewrite(files: Seq[FileToRewrite], pass: Int): Int = {
    val totalBytes = files.map(_.size).sum
    logger.info(s"Pass $pass: rewriting ${files.size} files (${formatBytes(totalBytes)}) with $threads threads: " +
      files.groupBy(_.columnFamily.collection).map { case (collection, filesOfCollection) =>
        s"$collection: ${filesOfCollection.size} files, ${formatBytes(filesOfCollection.map(_.size).sum)}"
      }.mkString("; "))

    val start = System.nanoTime()
    val queue = new DelayQueue[Scheduled]()
    files.foreach(file => queue.add(new Scheduled(file, 0)))
    val remainingFiles = new AtomicInteger(files.size)
    val processedFiles = new AtomicInteger(0)
    val rewrittenFiles = new AtomicInteger(0)
    val processedBytes = new AtomicLong(0)
    val failure = new AtomicReference[Throwable]()

    def worker(): Unit =
      while (remainingFiles.get() > 0 && failure.get() == null && !closing.get()) {
        Option(queue.poll(100, TimeUnit.MILLISECONDS)).foreach { scheduled =>
          val file = scheduled.file
          val fileStart = System.nanoTime()
          whileOpen(rewriteFile(file)) match {
            case Busy =>
              logger.debug(s"$file is being compacted, retrying later")
              queue.add(new Scheduled(file, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(busyRetryDelayMillis)))
            case result =>
              val processed = processedFiles.incrementAndGet()
              val bytes = processedBytes.addAndGet(file.size)
              val elapsed = System.nanoTime() - start
              val remaining = if (bytes > 0) (elapsed.toDouble * (totalBytes - bytes) / bytes).toLong else 0L
              val action = result match {
                case Rewritten(outputFiles) =>
                  rewrittenFiles.incrementAndGet()
                  s"Rewrote $file in ${formatDuration(System.nanoTime() - fileStart)} to ${outputFiles.map(baseName).mkString(", ")}"
                case _ => s"Skipped $file, a compaction rewrote it meanwhile"
              }
              logger.info(s"$action. Progress: $processed/${files.size} files, ${formatBytes(bytes)}/${formatBytes(totalBytes)} " +
                s"(${if (totalBytes > 0) bytes * 100 / totalBytes else 100}%), ${formatDuration(elapsed)} elapsed, about " +
                s"${formatDuration(remaining)} left")
              remainingFiles.decrementAndGet()
          }
        }
      }

    val threadCount = new AtomicInteger(0)
    val executor = Executors.newFixedThreadPool(threads, new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, s"compact-all-data-${threadCount.incrementAndGet()}")
        thread.setDaemon(true)
        thread
      }
    })
    try {
      val workers = (1 to threads).map(_ => executor.submit(new Runnable {
        override def run(): Unit = try worker() catch {
          case e: Throwable => failure.compareAndSet(null, e)
        }
      }))
      workers.foreach(_.get())
    } finally {
      executor.shutdownNow()
    }

    Option(failure.get()).foreach(e => throw new Exception(s"Compaction failed: ${e.getMessage}", e))
    if (remainingFiles.get() > 0) throw new Exception("Compaction aborted because the database is closing")
    logger.info(s"Pass $pass done: rewrote ${rewrittenFiles.get()} files, skipped ${processedFiles.get() - rewrittenFiles.get()} " +
      s"in ${formatDuration(System.nanoTime() - start)}")
    rewrittenFiles.get()
  }

  private sealed trait RewriteResult
  private case class Rewritten(outputFiles: Seq[String]) extends RewriteResult
  private case object Gone extends RewriteResult
  private case object Busy extends RewriteResult

  private def rewriteFile(file: FileToRewrite): RewriteResult =
    currentLevel(file) match {
      case None => Gone
      // A trivial move may have put the file into another level, where its compression may be fine
      case Some(level) if level != file.level && !needsRewrite(file.columnFamily.options, file.compression, isBottommost(file.columnFamily, level)) => Gone
      case Some(level) =>
        try {
          Using.resource(new CompactionOptions()) { compactionOptions =>
            // The default output compression (DISABLE_COMPRESSION_OPTION) means: use the column family's compression
            // for the output level, as an automatic compaction would.
            Rewritten(db.compactFiles(compactionOptions, file.columnFamily.handle, List(file.fileName).asJava, level, -1, null).asScala.toList)
          }
        } catch {
          case e: RocksDBException =>
            if (currentLevel(file).isEmpty) Gone
            // An automatic compaction holds the file (or writes to its key range), it is probably gone afterwards
            else if (Option(e.getStatus).exists(status => Set(Status.Code.Aborted, Status.Code.Busy, Status.Code.TryAgain).contains(status.getCode))) Busy
            else throw e
        }
    }

  private def whileOpen[T](block: => T): T = {
    closeLock.readLock().lock()
    try {
      if (closing.get()) throw new Exception("Compaction aborted because the database is closing")
      block
    } finally {
      closeLock.readLock().unlock()
    }
  }

  private def currentLevel(file: FileToRewrite): Option[Int] =
    db.getColumnFamilyMetaData(file.columnFamily.handle).levels().asScala.collectFirst {
      case level if level.files().asScala.exists(_.fileName() == file.fileName) => level.level()
    }

  private def isBottommost(cf: ColumnFamily, level: Int): Boolean =
    db.getColumnFamilyMetaData(cf.handle).levels().asScala.filter(!_.files().isEmpty).map(_.level()).maxOption.forall(level >= _)

  private def needsRewrite(options: ColumnFamilyOptions, fileCompression: String, isBottommost: Boolean): Boolean =
    FileByFileCompaction.needsRewrite(options, fileCompression, isBottommost)

  private def baseName(path: String): String = Paths.get(path).getFileName.toString

  private def formatBytes(bytes: Long): String =
    if (bytes >= 1000L * 1000 * 1000) f"${bytes / 1e9}%.1f GB" else s"${bytes / 1000000} MB"

  private def formatDuration(nanos: Long): String = {
    val seconds = TimeUnit.NANOSECONDS.toSeconds(nanos)
    if (seconds >= 3600) f"${seconds / 3600}h ${seconds % 3600 / 60}%02dm"
    else if (seconds >= 60) f"${seconds / 60}m ${seconds % 60}%02ds"
    else f"${nanos / 1e9}%.1fs"
  }

  private class Scheduled(val file: FileToRewrite, val notBeforeNanos: Long) extends Delayed {
    override def getDelay(unit: TimeUnit): Long = unit.convert(notBeforeNanos - System.nanoTime(), TimeUnit.NANOSECONDS)

    override def compareTo(other: Delayed): Int = other match {
      case o: Scheduled => Ordering[(Long, Int)].compare((notBeforeNanos, file.order), (o.notBeforeNanos, o.file.order))
      case _ => java.lang.Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS))
    }
  }
}

object FileByFileCompaction {

  // As written to the table properties of SST files (CompressionTypeToString in RocksDB)
  private val compressionNames: Map[CompressionType, String] = Map(
    CompressionType.NO_COMPRESSION -> "NoCompression",
    CompressionType.SNAPPY_COMPRESSION -> "Snappy",
    CompressionType.ZLIB_COMPRESSION -> "Zlib",
    CompressionType.BZLIB2_COMPRESSION -> "BZip2",
    CompressionType.LZ4_COMPRESSION -> "LZ4",
    CompressionType.LZ4HC_COMPRESSION -> "LZ4HC",
    CompressionType.XPRESS_COMPRESSION -> "Xpress",
    CompressionType.ZSTD_COMPRESSION -> "ZSTD"
  )

  /**
   * Whether a file has a compression that a compaction into its level would not use with the given options.
   * In the bottommost level, that is the bottommost compression, if set. Elsewhere, it is any of the configured
   * compressions: RocksDB chooses the one of a level from compression_per_level relative to the current base level,
   * which isn't known here, and only outdated compressions (e.g. snappy from older FossilDB versions) matter anyway.
   */
  def needsRewrite(options: ColumnFamilyOptions, fileCompression: String, isBottommost: Boolean): Boolean = {
    val bottommost = Some(options.bottommostCompressionType()).filter(_ != CompressionType.DISABLE_COMPRESSION_OPTION)
    if (isBottommost && bottommost.isDefined)
      !compressionNames.get(bottommost.get).contains(fileCompression)
    else {
      val configured = (options.compressionType() +: options.compressionPerLevel().asScala.toSeq) ++ bottommost
      !configured.flatMap(compressionNames.get).contains(fileCompression)
    }
  }
}
