package com.app

import com.Utils.SparkSessionWrapper
import com.join.DataJoin
import com.preprocess.flights.FlightPreprocessing
import com.preprocess.weather.WeatherPreprocessing
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.io.StdIn

object DataProcessingApplication {
  private val spark: SparkSession = SparkSessionWrapper.spark

  def main(args: Array[String]): Unit = {

    val currentPath = Paths.get("").toAbsolutePath
    println(s"Current directory is: $currentPath")

    println("Enter root path:")
    val hdfsPath = StdIn.readLine()
    println(s"Path is: $hdfsPath")
    val basePaths = Map(
      "flight" -> "FlightsLight",
      "weather" -> "WeatherLight",
      "table_finale" -> "TableML"
    )

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

    println("Enter number of partitions for join:")
    val partitionsForJoin = StdIn.readInt()

    val flightsPreprocessing = new FlightPreprocessing(rawFlightsData, timezoneData)
    flightsPreprocessing.buildFlightTable()
    val flights = flightsPreprocessing.getProcessedFlightData
    val airports = flightsPreprocessing.getAirportList
    val weatherPreprocessing = new WeatherPreprocessing(rawWeatherData, timezoneData, airports)
    weatherPreprocessing.buildWeatherTable()
    val weather = weatherPreprocessing.getProcessedWeatherData
    val dataJoin = new DataJoin(dbfsDirs("table_finale"),partitionsForJoin, weather, flights )
    dataJoin.executePipeline()
  }

}