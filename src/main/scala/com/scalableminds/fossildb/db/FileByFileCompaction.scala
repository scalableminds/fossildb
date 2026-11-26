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
 * Rewriting single files keeps each compaction short (about 15 s per 1 GB file). Each rewrite counts as a running
 * compaction, and while compactions keep up, RocksDB schedules only one at a time, so automatic compactions wait
 * meanwhile and level 0 grows by a few files. Once it reaches the speed-up threshold (8 files with the default
 * options), RocksDB runs more compactions in parallel, and automatic compactions use the slots this leaves free,
 * well before RocksDB would slow down writes (20 files).
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
  // A file that another compaction holds this long (10 minutes) is left for the next pass, so a stuck file can't
  // keep CompactAllData running forever. Warn every minute while retrying.
  private val maxBusyRetries = 300
  private val busyWarnEveryRetries = 30

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
      logger.warn(s"${filesToRewrite.size} files (${formatBytes(filesToRewrite.map(_.size).sum)}) still have an outdated " +
        s"compression after $maxPasses passes (they were added during compaction, or other compactions held them), they " +
        s"are left as they are. Run CompactAllData again to rewrite them.")
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
    // Not 0, System.nanoTime() may be negative
    files.foreach(file => queue.add(new Scheduled(file, start)))
    val remainingFiles = new AtomicInteger(files.size)
    val processedFiles = new AtomicInteger(0)
    val rewrittenFiles = new AtomicInteger(0)
    val gaveUpFiles = new AtomicInteger(0)
    val processedBytes = new AtomicLong(0)
    val failure = new AtomicReference[Throwable]()
    // Set when this returns, e.g. if the calling thread is interrupted
    val stopped = new AtomicBoolean(false)

    def worker(): Unit =
      while (remainingFiles.get() > 0 && failure.get() == null && !closing.get() && !stopped.get()) {
        Option(queue.poll(100, TimeUnit.MILLISECONDS)).foreach { scheduled =>
          val file = scheduled.file
          val fileStart = System.nanoTime()
          val busyFor = s"${formatDuration(TimeUnit.MILLISECONDS.toNanos(busyRetryDelayMillis * (scheduled.retries + 1)))}"
          (whileOpen(rewriteFile(file)) match {
            case Busy if scheduled.retries >= maxBusyRetries => GaveUp
            case result => result
          }) match {
            case Busy =>
              val retries = scheduled.retries + 1
              if (retries % busyWarnEveryRetries == 0)
                logger.warn(s"Other compactions have held $file for about $busyFor, still retrying")
              else
                logger.debug(s"$file is being compacted, retrying later")
              queue.add(new Scheduled(file, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(busyRetryDelayMillis), retries))
            case result =>
              val processed = processedFiles.incrementAndGet()
              val bytes = processedBytes.addAndGet(file.size)
              val elapsed = System.nanoTime() - start
              val remaining = if (bytes > 0) (elapsed.toDouble * (totalBytes - bytes) / bytes).toLong else 0L
              val action = result match {
                case Rewritten(outputFiles) =>
                  rewrittenFiles.incrementAndGet()
                  s"Rewrote $file in ${formatDuration(System.nanoTime() - fileStart)} to ${outputFiles.map(baseName).mkString(", ")}"
                case GaveUp =>
                  gaveUpFiles.incrementAndGet()
                  s"Gave up on $file, other compactions held it for about $busyFor. The next pass or call retries it"
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
      // Don't return while workers still use the database, e.g. if this thread is interrupted. A worker in CompactFiles
      // (a native call, which ignores interrupts) finishes its file, then stops.
      stopped.set(true)
      executor.shutdownNow()
      var interrupted = false
      while (!executor.isTerminated)
        try executor.awaitTermination(1, TimeUnit.SECONDS) catch {
          case _: InterruptedException => interrupted = true
        }
      if (interrupted) Thread.currentThread().interrupt()
    }

    Option(failure.get()).foreach(e => throw new Exception(s"Compaction failed: ${e.getMessage}", e))
    if (remainingFiles.get() > 0) throw new Exception("Compaction aborted because the database is closing")
    logger.info(s"Pass $pass done: rewrote ${rewrittenFiles.get()} files, skipped " +
      s"${processedFiles.get() - rewrittenFiles.get() - gaveUpFiles.get()}, gave up on ${gaveUpFiles.get()} that other " +
      s"compactions held, in ${formatDuration(System.nanoTime() - start)}")
    rewrittenFiles.get()
  }

  private sealed trait RewriteResult
  private case class Rewritten(outputFiles: Seq[String]) extends RewriteResult
  private case object Gone extends RewriteResult
  private case object Busy extends RewriteResult
  private case object GaveUp extends RewriteResult

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
            val code = Option(e.getStatus).map(_.getCode)
            if (closing.get() || code.contains(Status.Code.ShutdownInProgress))
              throw new Exception("Compaction aborted because the database is closing", e)
            currentLevel(file) match {
              case None => Gone
              // A trivial move put the file into another level between the check above and CompactFiles (which then
              // fails with InvalidArgument, as it can't compact to a higher level). Check the file again later.
              case Some(newLevel) if newLevel != level => Busy
              // An automatic compaction holds the file (or writes to its key range), it is probably gone afterwards
              case _ if code.exists(Set(Status.Code.Aborted, Status.Code.Busy, Status.Code.TryAgain).contains) => Busy
              case _ => throw e
            }
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

  // retries: how often the file was retried because another compaction held it
  private class Scheduled(val file: FileToRewrite, val notBeforeNanos: Long, val retries: Int = 0) extends Delayed {
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
