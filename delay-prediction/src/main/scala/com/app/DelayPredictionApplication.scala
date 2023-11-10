package com

import org.apache.log4j.Level._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.models.Model

import java.nio.file.Paths

object DelayPredictionApplication {
  Logger.getLogger("org").setLevel(OFF)
  Logger.getLogger("akka").setLevel(OFF)
  private case class UserInputs(hdfsPathML: String, originTimeRange: Int, destinationTimeRange: Int, delayThreshold: Int, numTreesForRandomForest: Int)
  private def collectUserInputs(): UserInputs = {
    val ANSI_RESET = "\u001B[0m"
    val ANSI_BOLD = "\u001B[1m"
    val ANSI_CYAN = "\u001B[36m"
    val ANSI_YELLOW = "\u001B[33m"
    println(ANSI_BOLD + "===================================" + ANSI_RESET)
    println(ANSI_BOLD + "Welcome to the Delay Prediction App" + ANSI_RESET)
    println(ANSI_BOLD + "===================================" + ANSI_RESET)
    println(ANSI_CYAN + "Instructions:" + ANSI_RESET)
    println("* Enter the root path for your processed data.")
    println("   If you did not process data, you can use already processed data from:")
    println(ANSI_BOLD + "  /students/iasd_20222023/asakhraoui/output"+ ANSI_RESET)
    println()
    val currentPath = Paths.get("").toAbsolutePath
    println(ANSI_CYAN + "Current directory is:" + ANSI_RESET)
    println(s"$ANSI_YELLOW$currentPath$ANSI_RESET")

    println(ANSI_CYAN + "Enter your processed data path:" + ANSI_RESET)
    val hdfsPathML = scala.io.StdIn.readLine()
    println(s"Processed data path: $ANSI_YELLOW$hdfsPathML$ANSI_RESET")

    println(ANSI_CYAN +"Enter the time range before CRS (between 0 and 11 included):"+ ANSI_RESET) //0-12
    val originTimeRange = scala.io.StdIn.readLine().toInt

    println(ANSI_CYAN +"Enter the time range after Scheduled Arrival (between 0 and 11 included):"+ ANSI_RESET) ////0-12
    val destinationTimeRange = scala.io.StdIn.readLine().toInt

    println(ANSI_CYAN +"Enter the delay threshold in minutes (between 15 and 60):"+ ANSI_RESET)//15-60
    val delayThreshold = scala.io.StdIn.readLine().toInt

    println(ANSI_CYAN +"Enter the number of trees for RandomForest (recommended: 60):"+ ANSI_RESET) //100
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
    val handleInvalid = "skip"

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
