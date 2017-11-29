package com.scalableminds.fossildb

import java.nio.file.Paths

import com.scalableminds.fossildb.db.StoreManager
import com.typesafe.scalalogging.LazyLogging
import fossildb.BuildInfo

import scala.concurrent.ExecutionContext

object ConfigDefaults {val port = 7155; val dataDir = "data"; val backupDir = "backup"; val columnFamilies = List()}
case class Config(port: Int = ConfigDefaults.port, dataDir: String = ConfigDefaults.dataDir,
                  backupDir: String = ConfigDefaults.backupDir, columnFamilies: List[String] = ConfigDefaults.columnFamilies)

object FossilDB extends LazyLogging {
  def main(args: Array[String]) = {

    parseArguments(args) match {
      case Some(config) => {
        logger.info("Starting FossilDB")
        logger.info("BuildInfo: (" + BuildInfo + ")")
        logger.info("Config: " + config)

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
        c.copy(port = x) ).text("port to listen on. Default: " + ConfigDefaults.port)

      opt[String]('d', "dataDir").valueName("<path>").action( (x, c) =>
        c.copy(dataDir = x) ).text("database directory. Default: " + ConfigDefaults.dataDir)

      opt[String]('b', "backupDir").valueName("<path>").action( (x, c) =>
        c.copy(backupDir = x) ).text("backup directory. Default: " + ConfigDefaults.backupDir)

      opt[Seq[String]]('c', "columnFamilies").valueName("<cf1>,<cf2>...").action( (x, c) =>
        c.copy(columnFamilies = x.toList) ).required.text("column families of the database (created if there is no db yet)")
    }

    parser.parse(args, Config())
  }
}
