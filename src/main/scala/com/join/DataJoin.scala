package com.join

import com.Utils.SparkSessionWrapper
import com.preprocess.flights.{Flight, FlightPreprocessing}
import com.preprocess.weather.{Weather, WeatherPreprocessing}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

import java.nio.file.Paths
import scala.io.StdIn
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object DataJoin {
  private val spark: SparkSession = SparkSessionWrapper.spark

  import spark.implicits._

  private var flights: DataFrame = _
  private var airportList: DataFrame = _
  private var weather: DataFrame = _

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
    val flights = flightsPreprocessing.buildFlightTable()
    val airports = flightsPreprocessing.getAirportList
    val weatherPreprocessing = new WeatherPreprocessing(rawWeatherData, timezoneData, airports)
  }

  def createDataSet(table: DataFrame, dataType: Encoder[_]): Dataset[_] = table.as(dataType)

  def getCurrentTime: Long = System.currentTimeMillis() / 1000

  def createFTOrigin(table_flight_DS: Dataset[Flight]): DataFrame = {
  }

  def createOT(table_weather_DS: Dataset[Weather]): DataFrame = {
  }

  def repartitionDataFrames(OT: DataFrame, FT_Origin: DataFrame, numPartitions: Int): (DataFrame, DataFrame) = {
    val OT_partition = OT.repartition(numPartitions, $"OT_KEY".getItem("_1")).persist
    val FT_Origin_partition = FT_Origin.repartition(numPartitions, $"FT_KEY".getItem("_1"))
    (OT_partition, FT_Origin_partition)
  }

  def joinOTAndFT(OT_partition: DataFrame, FT_Origin_partition: DataFrame): DataFrame = {
  }

  def aggregateFOT(FOT_joined_Origin: DataFrame): DataFrame = {
  }

  def createFTDestination(FOT_Etape_2: DataFrame): DataFrame = {
  }

}

object MainApp {
  private val spark: SparkSession = SparkSessionWrapper.spark

  import spark.implicits._

  def main(args: Array[String]): Unit = {



    //    data_join.loadData(hdfsPath)
//
//
//
//    val table_flight_DS = DataProcessing.createDataSet(Table_flights, Encoders.bean(classOf[Flights]))
//    val table_weather_DS = DataProcessing.createDataSet(table_weather, Encoders.bean(classOf[Weather]))
//
//    println(DataProcessing.getCurrentTime)
//
//    val FT_Origin = DataProcessing.createFTOrigin(table_flight_DS)
//    val OT = DataProcessing.createOT(table_weather_DS)
//
//    val (OT_partition, FT_Origin_partition) = DataProcessing.repartitionDataFrames(OT, FT_Origin, param1Value)
//
//    val FOT_joined_Origin = DataProcessing.joinOTAndFT(OT_partition, FT_Origin_partition)
//    val FOT_Etape_2 = DataProcessing.aggregateFOT(FOT_joined_Origin)
//    val FT_Destination = DataProcessing.createFTDestination(FOT_Etape_2)

  }
}
