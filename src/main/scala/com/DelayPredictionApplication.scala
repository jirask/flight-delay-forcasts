package com

import org.apache.log4j.Level._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.models.RandomForest
import org.apache.spark.sql.functions.col

import scala.io.StdIn

object DelayPredictionApplication {
  Logger.getLogger("org").setLevel(OFF)
  Logger.getLogger("akka").setLevel(OFF)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FlightsDelayPredictionML")
      .master("local[*]")
      .getOrCreate()
    //TODO finish getting the input user and correctly call the mdeol
    val dbfsDir_ML = "dbfs path"
    val ThresdholdDelay = 15
    val PlageHoraireOrigin = 3
    val PlageHoraireDestination = 3

//    val model = new RandomForest(spark, dbfsDir_ML, ThresdholdDelay, PlageHoraireOrigin, PlageHoraireDestination)
//    model.executePipeline()

  }
}
