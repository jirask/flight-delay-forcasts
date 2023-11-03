package com.join

import com.Utils.SparkSessionWrapper
import com.preprocess.flights.{Flight, FlightPreprocessing}
import com.preprocess.weather.{Weather, WeatherPreprocessing}
import org.apache.spark.sql.functions._

import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ArrayBuffer
import java.nio.file.Paths
import scala.io.StdIn
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession, functions}

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
    table_flight_DS
      .orderBy($"ORIGIN_AIRPORT_ID", to_date($"CRS_DEP_TIMESTAMP"))
      .flatMap { flight =>
        val dateCRSOrigin = new Date(flight.CRS_DEP_TIMESTAMP.getTime)
        val joinKey = (flight.ORIGIN_AIRPORT_ID, dateCRSOrigin)

        val flightRow = (
          flight.ORIGIN_AIRPORT_ID,
          flight.DEST_AIRPORT_ID,
          flight.CRS_DEP_TIMESTAMP,
          flight.SCHEDULED_ARRIVAL_TIMESTAMP,
          flight.ARR_DELAY_NEW
        )
        Seq((joinKey, "FT") -> flightRow)
      }
      .withColumnRenamed("_1", "FT_KEY")
      .withColumnRenamed("_2", "Flights_List")
  }

  def createOT(table_weather_DS: Dataset[Weather]): DataFrame = {
    table_weather_DS
      .orderBy($"AIRPORT_ID", $"Weather_TIMESTAMP".desc)
      .flatMap { weather =>
        val dateWeather = new Date(weather.Weather_TIMESTAMP.getTime)
        val joinKey = (weather.AIRPORT_ID, dateWeather)

        val weatherRow = (
          weather.AIRPORT_ID,
          weather.Weather_TIMESTAMP,
          weather.DryBulbCelsius,
          weather.SkyCondition,
          weather.Visibility,
          weather.WindDirection,
          weather.WindSpeed,
          weather.WeatherType,
          weather.StationPressure
        )

        val localDateTime = weather.Weather_TIMESTAMP.toLocalDateTime
        val datePlus12Hours = localDateTime.plus(12, ChronoUnit.HOURS).toLocalDate
        val datePlus1Day = localDateTime.plus(1, ChronoUnit.DAYS).toLocalDate

        if (datePlus12Hours == datePlus1Day) {
          Seq((joinKey, "OT") -> weatherRow, (weather.AIRPORT_ID, Date.valueOf(datePlus1Day), "OT") -> weatherRow)
        } else {
          Seq((joinKey, "OT") -> weatherRow)
        }
      }
      .withColumnRenamed("_1", "OT_KEY")
      .withColumnRenamed("_2", "OT_VALUE")
      .groupBy("OT_KEY")
      .agg(collect_list("OT_VALUE").as("Weather_List"))
      .filter(functions.size($"Weather_List") === 36)
  }


  def repartitionDataFrames(OT: DataFrame, FT_Origin: DataFrame, numPartitions: Int): (DataFrame, DataFrame) = {
    val OT_partition = OT.repartition(numPartitions, $"OT_KEY".getItem("_1")).persist
    val FT_Origin_partition = FT_Origin.repartition(numPartitions, $"FT_KEY".getItem("_1"))
    (OT_partition, FT_Origin_partition)
  }

  def joinOTAndFT(OT_partition: DataFrame, FT_Origin_partition: DataFrame): DataFrame = {
    OT_partition
      .join(FT_Origin_partition, OT_partition("OT_KEY")
        .getItem("_1") === FT_Origin_partition("FT_KEY")
        .getItem("_1"), "inner")

  }

  def rowToWeather(row: Row): Weather = {
    Weather(
      AIRPORT_ID = row.getInt(0),
      Weather_TIMESTAMP = row.getTimestamp(1),
      DryBulbCelsius = row.getDouble(2),
      SkyCondition = row.getString(3),
      Visibility = row.getDouble(4),
      WindDirection = row.getInt(5),
      WindSpeed = row.getDouble(6),
      WeatherType = row.getString(7),
      StationPressure = row.getDouble(8)
    )
  }

  def aggregateFOT(FOT_joined_Origin: DataFrame): DataFrame = {
    FOT_joined_Origin
      .map { row =>
        val weatherList = row.getAs[Seq[Row]](1)
        val flightInfo = Flight(
          ORIGIN_AIRPORT_ID = row.getStruct(3).getInt(0),
          DEST_AIRPORT_ID = row.getStruct(3).getInt(1),
          CRS_DEP_TIMESTAMP = row.getStruct(3).getTimestamp(2),
          SCHEDULED_ARRIVAL_TIMESTAMP = row.getStruct(3).getTimestamp(3),
          ARR_DELAY_NEW = row.getStruct(3).getDouble(4)
        )


        // etape 1: convertir le java.sql.timestamp en java.time.LocalDateTime
        val timestamp_CRS_Departure = flightInfo.CRS_DEP_TIMESTAMP.toLocalDateTime
        //Etapes 2 & 3 : retirer 12h et convertir le résultat de java.time.LocalDateTime en java.sql.Timestamp
        val twelveHoursBeforeCRSDeparture = Timestamp.valueOf(flightInfo.CRS_DEP_TIMESTAMP.toLocalDateTime.minus(12, ChronoUnit.HOURS))

        val sortedWeatherList = weatherList.sortWith { (row1, row2) => row1.getAs[java.sql.Timestamp]("_2").before(row2.getAs[java.sql.Timestamp]("_2"))
        }.map(rowToWeather)
        val relevantWeatherData = sortedWeatherList.collect {
          case weather if twelveHoursBeforeCRSDeparture.before(weather.Weather_TIMESTAMP) &&
            (weather.Weather_TIMESTAMP.before(flightInfo.CRS_DEP_TIMESTAMP) ||
              weather.Weather_TIMESTAMP.equals(flightInfo.CRS_DEP_TIMESTAMP)) => weather
        }
        (flightInfo, relevantWeatherData)
      }
      .withColumnRenamed("_1", "FT")
      .withColumnRenamed("_2", "WO_List")
  }

  def createFTDestination(FOT_Etape_2: DataFrame): DataFrame = {
    FOT_Etape_2.flatMap { row =>
        val weatherList = row.getAs[Seq[Row]](1).map(rowToWeather).toArray
        val flightInfo = Flight(
          ORIGIN_AIRPORT_ID = row.getStruct(0).getInt(0),
          DEST_AIRPORT_ID = row.getStruct(0).getInt(1),
          CRS_DEP_TIMESTAMP = row.getStruct(0).getTimestamp(2),
          SCHEDULED_ARRIVAL_TIMESTAMP = row.getStruct(0).getTimestamp(3),
          ARR_DELAY_NEW = row.getStruct(0).getDouble(4)
        )
        val dateCRSDestination = new Date(flightInfo.SCHEDULED_ARRIVAL_TIMESTAMP.getTime)
        val joinKey = (flightInfo.DEST_AIRPORT_ID, dateCRSDestination)
        Seq(((joinKey, "FT"), flightInfo, weatherList))
      }
      .withColumnRenamed("_1", "FT_KEY")
      .withColumnRenamed("_2", "Flight_List")
      .withColumnRenamed("_3", "Weather_List_Origin")
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
