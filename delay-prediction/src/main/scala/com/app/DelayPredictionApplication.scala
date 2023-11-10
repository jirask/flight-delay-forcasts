package com

import org.apache.log4j.Level._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.models.Model

object DelayPredictionApplication {
  Logger.getLogger("org").setLevel(OFF)
  Logger.getLogger("akka").setLevel(OFF)
  case class UserInputs(hdfsPathML: String, originTimeRange: Int, destinationTimeRange: Int, delayThreshold: Int, numTreesForRandomForest: Int)
  private def collectUserInputs(): UserInputs = {
    println("Enter the path to the processed data:")
    val hdfsPathML = scala.io.StdIn.readLine()

    println(s"Path to the ML table: $hdfsPathML")

    println("Enter the time range before CRS (Central Reservation System):")
    val originTimeRange = scala.io.StdIn.readLine().toInt

    println("Enter the time range after Scheduled Arrival:")
    val destinationTimeRange = scala.io.StdIn.readLine().toInt

    println("Enter the delay threshold in minutes:")
    val delayThreshold = scala.io.StdIn.readLine().toInt

    println("Enter the number of trees for RandomForest:")
    val numTreesForRandomForest = scala.io.StdIn.readLine().toInt

    println(s"Origin time range considered: $originTimeRange")
    println(s"Destination time range considered: $destinationTimeRange")
    println(s"Delay threshold: $delayThreshold")
    println(s"Number of Trees for RandomForest: $numTreesForRandomForest")

    UserInputs(hdfsPathML, originTimeRange, destinationTimeRange, delayThreshold, numTreesForRandomForest)
  }
  def main(args: Array[String]): Unit = {
    val userInputs = collectUserInputs()

    val spark = SparkSession.builder()
      .appName("FlightsDelayPredictionML")
      .master("local[*]")
      .getOrCreate()
    val maxCat = 270
    val handleInvalid = "keep"

    val model = new Model(
      userInputs.hdfsPathML,
      userInputs.delayThreshold,
      userInputs.originTimeRange,
      userInputs.destinationTimeRange,
      maxCat,
      handleInvalid,
      userInputs.numTreesForRandomForest
    )
    model.executePipeline()
  }
}
