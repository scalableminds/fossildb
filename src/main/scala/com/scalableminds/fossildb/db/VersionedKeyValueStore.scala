package com.scalableminds.fossildb.db

import scala.annotation.tailrec
import scala.util.Try


case class VersionedKey(key: String, version: Long) {
  override def toString: String = s"$key@${(~version).toHexString.toUpperCase}@$version"
}

object VersionedKey {

  def apply(key: String): Option[VersionedKey] = {
    val parts = key.split('@')
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

class KeyOnlyIterator[T](underlying: RocksDBStore, startAfterKey: Option[String]) extends Iterator[String] {

  /*
     Note that seek in the underlying iterators either hits precisely or goes to the
     lexicographically *next* key. To achieve correct behavior with startAfterKey,
     we have to advance once in case of the exact hit.
   */

  private var currentKey: Option[String] = startAfterKey

  private def compositeKeyFor(keyOpt: Option[String]) = keyOpt match {
    case Some(key) => VersionedKey(key, 0).toString
    case None => ""
  }

  override def hasNext: Boolean = {
    val it = underlying.scanKeysOnly(compositeKeyFor(currentKey), None)
    if (it.hasNext && currentKey.isDefined && currentKey.contains(VersionedKey(it.peek).get.key)) it.next
    it.hasNext
  }

  override def next(): String = {
    val it = underlying.scanKeysOnly(compositeKeyFor(currentKey), None)
    if (it.hasNext && currentKey.isDefined && currentKey.contains(VersionedKey(it.peek).get.key)) it.next
    val nextKey = VersionedKey(it.next).get.key
    currentKey = Some(nextKey)
    nextKey
  }

}


class VersionedKeyValueStore(underlying: RocksDBStore) {

  def get(key: String, version: Option[Long] = None): Option[VersionedKeyValuePair[Array[Byte]]] =
    scanVersionValuePairs(key, version).toStream.headOption

  def getUnderlying: RocksDBStore = underlying

  def getMultipleVersions(key: String, oldestVersion: Option[Long] = None, newestVersion: Option[Long] = None): (List[Array[Byte]], List[Long]) = {

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

    val iterator = scanVersionValuePairs(key, newestVersion)
    val (versions, keys) = toListIter(iterator, List(), List())
    (versions.reverse, keys.reverse)
  }

  private def scanVersionValuePairs(key: String, version: Option[Long] = None): Iterator[VersionedKeyValuePair[Array[Byte]]] = {
    requireValidKey(key)
    val prefix = s"$key@"
    underlying.scan(version.map(VersionedKey(key, _).toString).getOrElse(prefix), Some(prefix)).flatMap { pair =>
      VersionedKey(pair.key).map(VersionedKeyValuePair(_, pair.value))
    }
  }

  private def scanVersionsOnly(key: String, version: Option[Long] = None): Iterator[VersionedKey] = {
    requireValidKey(key)
    val prefix = s"$key@"
    underlying.scanKeysOnly(version.map(VersionedKey(key, _).toString).getOrElse(prefix), Some(prefix)).flatMap { key =>
      VersionedKey(key)
    }
  }

  def getMultipleKeys(key: String, prefix: Option[String] = None, version: Option[Long] = None, limit: Option[Int]): (Seq[String], Seq[Array[Byte]], Seq[Long]) = {
    requireValidKey(key)
    prefix.foreach{ p => requireValidKey(p)}
    val iterator: VersionFilterIterator = scanKeys(key, prefix, version)
    val asSequence = iterator.take(limit.getOrElse(Int.MaxValue)).toSeq
    val keys = asSequence.map(_.key)
    val values = asSequence.map(_.value)
    val versions = asSequence.map(_.version)
    (keys, values, versions)
  }

  private def scanKeys(key: String, prefix: Option[String] = None, version: Option[Long] = None): VersionFilterIterator =
    new VersionFilterIterator(underlying.scan(key, prefix), version)

  def deleteMultipleVersions(key: String, oldestVersion: Option[Long] = None, newestVersion: Option[Long] = None): Unit = {
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

    val versionsIterator = scanVersionsOnly(key, newestVersion)
    deleteIter(versionsIterator)
  }

  def put(key: String, version: Long, value: Array[Byte]): Unit = {
    requireValidKey(key)
    underlying.put(VersionedKey(key, version).toString, value)
  }

  def delete(key: String, version: Long): Unit = {
    requireValidKey(key)
    underlying.delete(VersionedKey(key, version).toString)
  }

  def listKeys(limit: Option[Int], startAfterKey: Option[String]): Seq[String] = {
    val iterator = new KeyOnlyIterator(underlying, startAfterKey)
    iterator.take(limit.getOrElse(Int.MaxValue)).toSeq
  }

  def listVersions(key: String, limit: Option[Int], offset: Option[Int]): Seq[Long] = {
    val iterator = scanVersionsOnly(key)
    iterator.map(_.version).drop(offset.getOrElse(0)).take(limit.getOrElse(Int.MaxValue)).toSeq
  }

  private def requireValidKey(key: String): Unit = {
    require(!(key contains "@"), "keys cannot contain the char @")
  }
}
