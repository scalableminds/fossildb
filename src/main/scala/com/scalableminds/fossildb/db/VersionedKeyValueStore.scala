/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.fossildb.db

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

  def key = versionedKey.key

  def version = versionedKey.version
}


class VersionFilterIterator[T](it: Iterator[KeyValuePair[T]], version: Option[Long]) extends Iterator[VersionedKeyValuePair[T]] {

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

  override def next(): VersionedKeyValuePair[T] = {
    val value = versionedIterator.next()
    currentKey = Some(value.key)
    value
  }
}


class VersionedKeyValueStore(underlying: RocksDBStore) {

  def get(key: String, version: Option[Long] = None): Option[VersionedKeyValuePair[Array[Byte]]] =
    scanVersions(key, version).toStream.headOption

  def getMultipleVersions(key: String, oldestVersion: Option[Long] = None, newestVersion: Option[Long] = None) = {

    def toListIter(versionIterator: Iterator[VersionedKeyValuePair[Array[Byte]]],
                   accValues: List[Array[Byte]], accVersions: List[Long]): (List[Array[Byte]], List[Long]) = {
      if (!versionIterator.hasNext) (accValues, accVersions)
      else {
        val item = versionIterator.next()
        if (item.version < oldestVersion.getOrElse(0L)) (accValues, accVersions)
        else toListIter(versionIterator, item.value :: accValues, item.version :: accVersions)
      }
    }

    val iterator = scanVersions(key, newestVersion)
    toListIter(iterator, List(), List())
  }

  private def scanVersions(key: String, version: Option[Long] = None): Iterator[VersionedKeyValuePair[Array[Byte]]] = {
    val prefix = s"$key@"
    underlying.scan(version.map(VersionedKey(key, _).toString).getOrElse(prefix), Some(prefix)).flatMap { pair =>
      VersionedKey(pair.key).map(VersionedKeyValuePair(_, pair.value))
    }
  }

  def getMultipleKeys(key: String, prefix: Option[String] = None, version: Option[Long] = None): (Seq[String], Seq[Array[Byte]]) = {
    val iterator: Iterator[VersionedKeyValuePair[Array[Byte]]] = scanKeys(key, prefix, version)
    val asSequence = iterator.toSeq
    val keys = asSequence.map(_.key)
    val values = asSequence.map(_.value)
    (keys, values)
  }

  private def scanKeys(key: String, prefix: Option[String] = None, version: Option[Long] = None): Iterator[VersionedKeyValuePair[Array[Byte]]] =
    new VersionFilterIterator(underlying.scan(key, prefix), version)

  def deleteMultipleVersions(key: String, oldestVersion: Option[Long] = None, newestVersion: Option[Long] = None) = {
    def deleteIter(versionIterator: Iterator[VersionedKeyValuePair[Array[Byte]]]): Unit = {
      if (versionIterator.hasNext) {
        val item = versionIterator.next()
        if (item.version >= oldestVersion.getOrElse(0L)) {
          delete(item.key, item.version)
          deleteIter(versionIterator)
        }
      }
    }

    deleteIter(scanVersions(key, newestVersion))
  }

  def put(key: String, version: Long, value: Array[Byte]): Unit =
    underlying.put(VersionedKey(key, version).toString, value)

  def delete(key: String, version: Long) =
    underlying.delete(VersionedKey(key, version).toString)

  def listKeys(limit: Option[Int], offset: Option[Int]) = {
    val iterator: Iterator[VersionedKeyValuePair[Array[Byte]]] = scanKeys("", None, None)
    iterator.map(_.key).drop(offset.getOrElse(0)).take(limit.getOrElse(100000)).toSeq
  }

}
