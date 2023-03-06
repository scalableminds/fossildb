package com.scalableminds.fossildb

import java.io.{BufferedWriter, File, FileWriter}

trait TestHelpers {

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  protected def writeToFile(file: File, content: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }

}
