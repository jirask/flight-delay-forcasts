package com

import com.join.DataJoin
import com.preprocess.flights.FlightPreprocessing
import com.preprocess.weather.WeatherPreprocessing
import org.apache.log4j.Level._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.io.StdIn

object DataProcessingApplication {
  Logger.getLogger("org").setLevel(OFF)
  Logger.getLogger("akka").setLevel(OFF)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataProcessing")
      .master("local[*]")
      .getOrCreate()
    val ANSI_RESET = "\u001B[0m"
    val ANSI_BOLD = "\u001B[1m"
    val ANSI_CYAN = "\u001B[36m"
    val ANSI_YELLOW = "\u001B[33m"
    println(ANSI_BOLD + "===================================" + ANSI_RESET)
    println(ANSI_BOLD + "Welcome to the Data Processing App" + ANSI_RESET)
    println(ANSI_BOLD + "===================================" + ANSI_RESET)
    println(ANSI_CYAN + "Instructions:" + ANSI_RESET)
    println("1. Enter the root path for your input data.")
    println("   If you do not have data in your current repository, you can use the data from:")
    println("   /students/iasd_20222023/szanutto/ProjectFlight")
    println("   Reminder: Your data should be in two repositories: ")
    println("flight data in /FlightsLight and weather data in /WeatherLight and airport timezone in root path.")
    println("2. Enter the root path for your output data.")
    println("   This is where the processed data will be saved in chunks.")
    println("   an example for the output path: /students/iasd_20222023/user/output")
    println()
    println(ANSI_CYAN + "Current directory is:" + ANSI_RESET)


    val currentPath = Paths.get("").toAbsolutePath
    println(s"$ANSI_YELLOW$currentPath$ANSI_RESET")
    println(ANSI_CYAN + "Enter your input path:" + ANSI_RESET)
    val hdfsPath = StdIn.readLine()
    println(s"Input path is: $ANSI_YELLOW$hdfsPath$ANSI_RESET")
    println()
    val basePaths = Map(
      "flight" -> "/FlightsLight",
      "weather" -> "/WeatherLight"
    )
    println(ANSI_CYAN + "Enter output path (example: /students/iasd_20222023/user/output):" + ANSI_RESET)
    val outputPath = StdIn.readLine()
    println(s"Output path is: $ANSI_YELLOW$outputPath$ANSI_RESET")


    val dbfsDirs = basePaths.map { case (key, value) => key -> s"$hdfsPath$value" }

    dbfsDirs.foreach { case (key, path) => println(s"Relative path for $key: $path") }

    val rawFlightsData = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dbfsDirs("flight"))

    val rawWeatherData = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dbfsDirs("weather"))


    val timezoneData = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(hdfsPath)

//    println("Enter number of partitions for join (recommended: 12):") ////12 et 20
//    val partitionsForJoin = StdIn.readInt()
    val partitionsForJoin = 12
    println("Processing Flight Table.... ")
    val flightsPreprocessing = new FlightPreprocessing(rawFlightsData, timezoneData)
    flightsPreprocessing.buildFlightTable()
    val flights = flightsPreprocessing.getProcessedFlightData
    val airports = flightsPreprocessing.getAirportList
    println("Flight Table processing is done. ")

    println("Processing Weather Table.... ")

    val weatherPreprocessing = new WeatherPreprocessing(rawWeatherData, timezoneData, airports)
    weatherPreprocessing.buildWeatherTable()
    val weather = weatherPreprocessing.getProcessedWeatherData
    println("Weather Table processing is done. ")


    println("Join.... ")
    val dataJoin = new DataJoin(outputPath,partitionsForJoin, weather, flights )
    dataJoin.executePipeline()
    println("Join is done. ")

  }

}