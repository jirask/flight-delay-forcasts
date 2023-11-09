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

    val currentPath = Paths.get("").toAbsolutePath
    println(s"Current directory is: $currentPath")

    println("Enter root path for your input data (example: /students/iasd_20222023/szanutto/ProjectFlight")
    val hdfsPath = StdIn.readLine()
    println(s"Path is: $hdfsPath")
    val basePaths = Map(
      "flight" -> "/FlightsLight",
      "weather" -> "/WeatherLight",
      "table_finale" -> "/TableML"
    )
    println("Enter output path (example: /students/iasd_20222023/szanutto")
    val outputPath = StdIn.readLine()

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

//    println("Enter number of partitions for join:")
//    val partitionsForJoin = StdIn.readInt()

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
    val dataJoin = new DataJoin(outputPath,12, weather, flights )
    dataJoin.executePipeline()
    println("Join is done. ")

  }

}