package com.Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSessionWrapper {
  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val session = SparkSession.builder()
      .appName("DataProcessing")
      .master("local[*]")
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    session
  }
}
