package com.scalableminds.fossildb

import java.nio.file.Paths

import com.scalableminds.fossildb.db.StoreManager
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext

case class Config(port: Int = 8090, dataDir: String = "data", backupDir: String = "backup", columnFamilies: List[String] = List())

object FossilDB extends LazyLogging {
  def main(args: Array[String]) = {

    parseArguments(args) match {
      case Some(config) => {
        logger.info("Starting FossilDB with config: " + config)

        val storeManager = new StoreManager(Paths.get(config.dataDir), Paths.get(config.backupDir), config.columnFamilies)

        val server = new FossilDBServer(storeManager, config.port, ExecutionContext.global)

        server.start()
        server.blockUntilShutdown()

      }
      case None => ()
    }

  }

  def parseArguments(args: Array[String]) = {
    val parser = new scopt.OptionParser[Config]("fossildb") {

      opt[Int]('p', "port").valueName("<num>").action( (x, c) =>
        c.copy(port = x) ).text("port to listen on")

      opt[String]('d', "dataDir").valueName("<path>").action( (x, c) =>
        c.copy(dataDir = x) ).text("database directory")

      opt[String]('b', "backupDir").valueName("<path>").action( (x, c) =>
        c.copy(backupDir = x) ).text("backup directory")

      opt[Seq[String]]('c', "columnFamilies").required.valueName("<cf1>,<cf2>...").action( (x, c) =>
        c.copy(columnFamilies = x.toList) ).text("column families of the database (created if there is no db yet)")
    }

    parser.parse(args, Config())
  }
}
