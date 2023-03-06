package com.scalableminds.fossildb

import java.nio.file.Paths

import com.scalableminds.fossildb.db.StoreManager
import com.typesafe.scalalogging.LazyLogging
import fossildb.BuildInfo

import scala.concurrent.ExecutionContext

object ConfigDefaults {
  val port: Int = 7155
  val dataDir: String = "data"
  val backupDir: String = "backup"
  val columnFamilies: List[String] = List()
  val rocksOptionsFile: Option[String] = None
  val chunkSize: Int = 1024 * 512 // 512 kb chunks
}
case class Config(port: Int = ConfigDefaults.port, dataDir: String = ConfigDefaults.dataDir,
                  backupDir: String = ConfigDefaults.backupDir, columnFamilies: List[String] = ConfigDefaults.columnFamilies,
                  rocksOptionsFile: Option[String] = ConfigDefaults.rocksOptionsFile, chunkSize: Int = ConfigDefaults.chunkSize)

object FossilDB extends LazyLogging {
  def main(args: Array[String]): Unit = {

    if (args.contains("--version"))
      println(BuildInfo.version)
    else {
      parseArguments(args) match {
        case Some(config) =>
          logger.info("Starting FossilDB")
          logger.info("BuildInfo: (" + BuildInfo + ")")
          logger.info("Config: " + config)

          val storeManager = new StoreManager(
            Paths.get(config.dataDir), Paths.get(config.backupDir), config.columnFamilies, config.rocksOptionsFile, config.chunkSize,
          )

          val server = new FossilDBServer(storeManager, config.port, ExecutionContext.global)

          server.start()
          server.blockUntilShutdown()
        case None => ()
      }
    }
  }

  private def parseArguments(args: Array[String]) = {
    val parser = new scopt.OptionParser[Config]("fossildb") {

      opt[Int]('p', "port").valueName("<num>").action( (x, c) =>
        c.copy(port = x) ).text("port to listen on. Default: " + ConfigDefaults.port)

      opt[String]('d', "dataDir").valueName("<path>").action( (x, c) =>
        c.copy(dataDir = x) ).text("database directory. Default: " + ConfigDefaults.dataDir)

      opt[String]('b', "backupDir").valueName("<path>").action( (x, c) =>
        c.copy(backupDir = x) ).text("backup directory. Default: " + ConfigDefaults.backupDir)

      opt[Seq[String]]('c', "columnFamilies").required.valueName("<cf1>,<cf2>...").action( (x, c) =>
        c.copy(columnFamilies = x.toList) ).text("column families of the database (created if there is no db yet)")

      opt[String]('r', "rocksOptionsFile").valueName("<filepath>").action((x, c) =>
        c.copy(rocksOptionsFile = Some(x))).text("rocksdb options file. Default: " + ConfigDefaults.rocksOptionsFile)

      opt[Int]('r', "chunkSize").valueName("<num>").action((x, c) =>
        c.copy(chunkSize = x)).text("Chunk size for splitting new large values. Default: " + ConfigDefaults.chunkSize)
    }

    parser.parse(args, Config())
  }
}
