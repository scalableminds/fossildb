/*
 * Copyright (C) 2011-2017 scalable minds UG (haftungsbeschränkt) & Co. KG. <http://scm.io>
 */
package com.scalableminds.kvservice.db

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


class VersionedKeyValueStore(underlying: RocksDBStore) {

  def get(key: String, version: Option[Long] = None): Option[VersionedKeyValuePair[Array[Byte]]] =
    scanVersions(key, version).toStream.headOption

  def scanVersions(key: String, version: Option[Long] = None): Iterator[VersionedKeyValuePair[Array[Byte]]] = {
    val prefix = s"$key@"
    underlying.scan(version.map(VersionedKey(key, _).toString).getOrElse(prefix), Some(prefix)).flatMap { pair =>
      VersionedKey(pair.key).map(VersionedKeyValuePair(_, pair.value))
    }
  }

/*  def scanKeys(key: String, prefix: Option[String] = None, version: Option[Long] = None): Iterator[VersionedKeyValuePair[Array[Byte]]] =
    new VersionFilterIterator(underlying.scan(key, prefix), version)*/

  def put(key: String, version: Long, value: Array[Byte]): Unit =
    underlying.put(VersionedKey(key, version).toString, value)

  def delete(key: String, version: Long) =
    underlying.delete(VersionedKey(key, version).toString)

}
