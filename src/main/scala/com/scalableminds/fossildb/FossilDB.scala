package com.scalableminds.fossildb

import java.nio.file.Paths

import com.scalableminds.fossildb.db.StoreManager
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object FossilDB {
  def main(args: Array[String]) = {

    val conf = ConfigFactory.load

    val dataDir = Paths.get(conf.getString("fossildb.dataDir"))
    val backupDir = Paths.get(conf.getString("fossildb.backupDir"))
    val columnFamilies = conf.getStringList("fossildb.columnFamilies").asScala.toList
    val port = conf.getInt("fossildb.port")

    val storeManager = new StoreManager(dataDir, backupDir, columnFamilies)

    val server = new StoreServer(storeManager, port, ExecutionContext.global)

    server.start()
    server.blockUntilShutdown()

  }
}
