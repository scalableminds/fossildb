package com.scalableminds.fossildb

import com.typesafe.scalalogging.Logger

object TimeLogger {
  def logTime[A](caption: String, logger: Logger, thresholdMillis: Long = 0L)(op: => A): A = {
    val before = System.currentTimeMillis()
    val result = op
    val durationMillis = System.currentTimeMillis() - before
    if (durationMillis >= thresholdMillis) {
      logger.info(s"TIMELOG | $caption took $durationMillis ms")
    }
    result
  }
}
