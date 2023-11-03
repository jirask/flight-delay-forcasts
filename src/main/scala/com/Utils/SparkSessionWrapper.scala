package com.Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.chaining.scalaUtilChainingOps

object SparkSessionWrapper {
  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    SparkSession.builder()
      .appName("DataProcessing")
      .master("local[*]")
      .getOrCreate()
      .tap(_.sparkContext.setLogLevel("ERROR"))
  }
}
