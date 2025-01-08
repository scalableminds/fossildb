package com.scalableminds.fossildb.db

import org.rocksdb.RocksIterator

import scala.annotation.tailrec
import scala.util.Try


case class VersionedKey(key: String, version: Long) {
  override def toString: String = s"$key${VersionedKey.versionSeparator}${(~version).toHexString.toUpperCase}${VersionedKey.versionSeparator}$version"
}

object VersionedKey {

  val versionSeparator: Char = '@'

  def apply(key: String): Option[VersionedKey] = {
    val parts = key.split(versionSeparator)
    for {
      key <- parts.headOption
      versionString <- parts.lastOption
      version <- Try(versionString.toLong).toOption
    } yield {
      VersionedKey(key, version)
    }
  }

}

case class VersionedKeyValuePair[T](versionedKey: VersionedKey, value: T) {

  def key: String = versionedKey.key

  def version: Long = versionedKey.version

}


class VersionFilterIterator(it: RocksDBIterator, version: Option[Long]) extends Iterator[VersionedKeyValuePair[Array[Byte]]] {

  private var currentKey: Option[String] = None

  private var versionedIterator = it.flatMap{ pair =>
    VersionedKey(pair.key).map(VersionedKeyValuePair(_, pair.value))
  }

  override def hasNext: Boolean = {
    versionedIterator = versionedIterator.dropWhile { pair =>
      currentKey.contains(pair.key) || version.exists(pair.version > _)
    }
    versionedIterator.hasNext
  }

  override def next(): VersionedKeyValuePair[Array[Byte]] = {
    val value = versionedIterator.next()
    currentKey = Some(value.key)
    value
  }

}

class KeyOnlyIterator[T](rocksIt: RocksIterator, startAfterKey: Option[String], prefix: Option[String]) extends Iterator[String] {

  /*
     Note that seek in the underlying iterators either hits precisely or goes to the
     lexicographically *next* key. To achieve correct behavior with startAfterKey,
     we have to advance once in case of the exact hit.
   */

  private var currentKey: Option[String] = startAfterKey

  private def compositeKeyFor(keyOpt: Option[String]) = keyOpt match {
    case Some(key) => VersionedKey(key, 0).toString
    // If the currentKey is not yet set, seek to the very beginning, or, if set, to the prefix.
    case None => prefix.getOrElse("")
  }

  override def hasNext: Boolean = {
    val it = RocksDBStore.scanKeysOnly(rocksIt, compositeKeyFor(currentKey), prefix)
    if (it.hasNext && currentKey.isDefined && currentKey.contains(VersionedKey(it.peek).get.key)) it.next()
    it.hasNext
  }

  override def next(): String = {
    val it = RocksDBStore.scanKeysOnly(rocksIt, compositeKeyFor(currentKey), prefix)
    if (it.hasNext && currentKey.isDefined && currentKey.contains(VersionedKey(it.peek).get.key)) it.next()
    val nextKey = VersionedKey(it.next()).get.key
    currentKey = Some(nextKey)
    nextKey
  }

}


class VersionedKeyValueStore(underlying: RocksDBStore) {

  def withRawRocksIterator[T](block: RocksIterator => T): T = underlying.withRawRocksIterator(block)

  def get(rocksIt: RocksIterator, key: String, version: Option[Long] = None): Option[VersionedKeyValuePair[Array[Byte]]] =
    scanVersionValuePairs(rocksIt, key, version).nextOption()

  def getMultipleVersions(rocksIt: RocksIterator, key: String, oldestVersion: Option[Long] = None, newestVersion: Option[Long] = None): (List[Array[Byte]], List[Long]) = {

    @tailrec
    def toListIter(versionIterator: Iterator[VersionedKeyValuePair[Array[Byte]]],
                   accValues: List[Array[Byte]], accVersions: List[Long]): (List[Array[Byte]], List[Long]) = {
      if (!versionIterator.hasNext) (accValues, accVersions)
      else {
        val item = versionIterator.next()
        if (item.version < oldestVersion.getOrElse(0L)) (accValues, accVersions)
        else toListIter(versionIterator, item.value :: accValues, item.version :: accVersions)
      }
    }

    val iterator = scanVersionValuePairs(rocksIt, key, newestVersion)
    val (versions, keys) = toListIter(iterator, List(), List())
    (versions.reverse, keys.reverse)
  }

  private def scanVersionValuePairs(rocksIt: RocksIterator, key: String, version: Option[Long] = None): Iterator[VersionedKeyValuePair[Array[Byte]]] = {
    requireValidKey(key)
    val prefix = s"$key${VersionedKey.versionSeparator}"
    RocksDBStore.scan(rocksIt, version.map(VersionedKey(key, _).toString).getOrElse(prefix), Some(prefix)).flatMap { pair =>
      VersionedKey(pair.key).map(VersionedKeyValuePair(_, pair.value))
    }
  }

  private def scanVersionsOnly(rocksIt: RocksIterator, key: String, version: Option[Long] = None): Iterator[VersionedKey] = {
    requireValidKey(key)
    val prefix = s"$key${VersionedKey.versionSeparator}"
    RocksDBStore.scanKeysOnly(rocksIt, version.map(VersionedKey(key, _).toString).getOrElse(prefix), Some(prefix)).flatMap { key =>
      VersionedKey(key)
    }
  }

  def getMultipleKeys(rocksIt: RocksIterator, startAfterKey: Option[String], prefix: Option[String] = None, version: Option[Long] = None, limit: Option[Int]): (Seq[String], Seq[Array[Byte]], Seq[Long]) = {
    startAfterKey.foreach(requireValidKey)
    prefix.foreach{ p => requireValidKey(p)}
    val iterator: VersionFilterIterator = scanKeys(rocksIt, startAfterKey, prefix, version)

    /*
       Note that seek in the underlying iterators either hits precisely or goes to the
       lexicographically *next* key. To achieve correct behavior with startAfterKey,
       we have to advance once in case of the exact hit.
     */
    val firstItemOpt: Option[VersionedKeyValuePair[Array[Byte]]] = if (iterator.hasNext) {
      val firstItem = iterator.next()
      if (startAfterKey.contains(firstItem.key)) {
        None
      } else {
        Some(firstItem)
      }
    } else None

    val limitPadded = limit.map(_ + 1).getOrElse(Int.MaxValue)
    val asVector = iterator.take(limitPadded).toVector
    val asSequenceAdvancedIfNeeded = firstItemOpt.map(_ +: asVector).getOrElse(asVector).take(limit.getOrElse(Int.MaxValue))
    val keys = asSequenceAdvancedIfNeeded.map(_.key)
    val values = asSequenceAdvancedIfNeeded.map(_.value)
    val versions = asSequenceAdvancedIfNeeded.map(_.version)
    (keys, values, versions)
  }

  private def scanKeys(rocksIt: RocksIterator, startAfterKey: Option[String], prefix: Option[String] = None, version: Option[Long] = None): VersionFilterIterator = {
    val fullKey = startAfterKey.map(key => s"$key${VersionedKey.versionSeparator}").orElse(prefix).getOrElse("")
    new VersionFilterIterator(RocksDBStore.scan(rocksIt, fullKey, prefix), version)
  }

  def deleteMultipleVersions(rocksIt: RocksIterator, key: String, oldestVersion: Option[Long] = None, newestVersion: Option[Long] = None): Unit = {
    @tailrec
    def deleteIter(versionIterator: Iterator[VersionedKey]): Unit = {
      if (versionIterator.hasNext) {
        val item = versionIterator.next()
        if (item.version >= oldestVersion.getOrElse(0L)) {
          delete(item.key, item.version)
          deleteIter(versionIterator)
        }
      }
    }

    val versionsIterator = scanVersionsOnly(rocksIt, key, newestVersion)
    deleteIter(versionsIterator)
  }

  def deleteAllByPrefix(rocksIt: RocksIterator, prefix: String): Unit = {
    RocksDBStore.scanKeysOnly(rocksIt, prefix, Some(prefix)).foreach(underlying.delete)
  }

  def put(key: String, version: Long, value: Array[Byte]): Unit = {
    requireValidKey(key)
    underlying.put(VersionedKey(key, version).toString, value)
  }

  def delete(key: String, version: Long): Unit = {
    requireValidKey(key)
    underlying.delete(VersionedKey(key, version).toString)
  }

  def listKeys(rocksIt: RocksIterator, limit: Option[Int], startAfterKey: Option[String], prefix: Option[String]): Seq[String] = {
    val iterator = new KeyOnlyIterator(rocksIt, startAfterKey, prefix)
    iterator.take(limit.getOrElse(Int.MaxValue)).toSeq
  }

  def listVersions(rocksIt: RocksIterator, key: String, limit: Option[Int], offset: Option[Int]): Seq[Long] = {
    val iterator = scanVersionsOnly(rocksIt, key)
    iterator.map(_.version).drop(offset.getOrElse(0)).take(limit.getOrElse(Int.MaxValue)).toSeq
  }

  private def requireValidKey(key: String): Unit = {
    require(!key.contains(VersionedKey.versionSeparator), s"keys cannot contain the char ${VersionedKey.versionSeparator}")
  }

}
