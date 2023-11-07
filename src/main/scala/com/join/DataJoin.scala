package com.join

import com.Utils.SparkSessionWrapper
import com.preprocess.flights.Flight
import com.preprocess.weather.Weather
import org.apache.spark.sql.functions._

import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

class DataJoin(joinedTablePath: String,
               NumberOfPartitions: Int,
               weather_df: DataFrame,
               flight_df: DataFrame) {
  private val spark: SparkSession = SparkSessionWrapper.spark

  import spark.implicits._

  def getCurrentTime: Long = System.currentTimeMillis() / 1000

  def createFlightsOriginDataset(flight_ds: Dataset[Flight]): DataFrame = {
    flight_ds
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

  def createWeatherOriginDataset(weather_ds: Dataset[Weather]): DataFrame = {
    weather_ds
      .orderBy($"AIRPORT_ID", $"Weather_TIMESTAMP".desc)
      .flatMap { weather =>
        val dateWeather = new Date(weather.Weather_TIMESTAMP.getTime)
        val joinKey = (weather.AIRPORT_ID, dateWeather)

        val weatherRow = (
          weather.AIRPORT_ID,
          weather.Weather_TIMESTAMP,
          weather.DryBulbCelsius,
          weather.SkyCOndition,
          weather.Visibility,
          weather.WindSpeed,
          weather.WeatherType,
          weather.HourlyPrecip
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

  def createWeatherOriginPartition(WeatherOrigin: DataFrame, numPartitions: Int) : DataFrame= {
    WeatherOrigin.repartition(numPartitions, $"OT_KEY".getItem("_1")).persist
  }

  def createFlightOriginPartition(FlightOrigin: DataFrame, numPartitions: Int): DataFrame = {
    FlightOrigin.repartition(numPartitions, $"FT_KEY".getItem("_1"))

  }

  def joinWeatherOriginAndFlightOrigin(WeatherOriginPartition: DataFrame, FlightOriginPartition: DataFrame): DataFrame = {
    WeatherOriginPartition
      .join(FlightOriginPartition, WeatherOriginPartition("OT_KEY")
        .getItem("_1") === FlightOriginPartition("FT_KEY")
        .getItem("_1"), "inner")

  }

  private def rowToWeather(row: Row): Weather = {
    Weather(
      AIRPORT_ID = row.getInt(0),
      Weather_TIMESTAMP = row.getTimestamp(1),
      DryBulbCelsius = row.getDouble(2),
      SkyCOndition = row.getString(3),
      Visibility = row.getDouble(4),
      WindSpeed = row.getDouble(5),
      WeatherType = row.getString(6),
      HourlyPrecip = row.getDouble(7)
    )
  }

  def aggregateFOT(WeatherOriginAndFlightOriginJoined: DataFrame): DataFrame = {
    WeatherOriginAndFlightOriginJoined
      .map { row =>
        val weatherList = row.getAs[Seq[Row]](1)
        val flightInfo = Flight(
          ORIGIN_AIRPORT_ID = row.getStruct(3).getInt(0),
          DEST_AIRPORT_ID = row.getStruct(3).getInt(1),
          CRS_DEP_TIMESTAMP = row.getStruct(3).getTimestamp(2),
          SCHEDULED_ARRIVAL_TIMESTAMP = row.getStruct(3).getTimestamp(3),
          ARR_DELAY_NEW = row.getStruct(3).getDouble(4)
        )

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

  def createFlightDestination(aggregatedFOT: DataFrame): DataFrame = {
    aggregatedFOT.flatMap { row =>
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

  def joinFlightsAndWeatherFinal(destFlights: DataFrame, weatherOriginPartitions: DataFrame, numPartitions: Int): DataFrame = {
    val partitionedDestFlights = destFlights.repartition(numPartitions, col("FT_KEY").getItem("_1"))

    val joinedData = weatherOriginPartitions.join(
      partitionedDestFlights,
      weatherOriginPartitions("OT_KEY").getItem("_1") === partitionedDestFlights("FT_KEY").getItem("_1"),
      joinType = "inner"
    )

    joinedData
  }


  def ProcessFlightWeatherTable(joinedFlightsAndWeatherFinal: DataFrame): DataFrame = {
    joinedFlightsAndWeatherFinal
      .map { row =>
        val weatherList = row.getAs[Seq[Row]](1)
        val flightInfo = Flight(
          ORIGIN_AIRPORT_ID = row.getStruct(3).getInt(0),
          DEST_AIRPORT_ID = row.getStruct(3).getInt(1),
          CRS_DEP_TIMESTAMP = row.getStruct(3).getTimestamp(2),
          SCHEDULED_ARRIVAL_TIMESTAMP = row.getStruct(3).getTimestamp(3),
          ARR_DELAY_NEW = row.getStruct(3).getDouble(4)
        )

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
      .withColumnRenamed("_3", "WD_List")
  }

  def createFinalTable(ProcessedFlightWeatherTable: DataFrame): DataFrame = {
    ProcessedFlightWeatherTable
      .withColumn("WO_11H", $"WO_List".getItem(0))
      .withColumn("WO_10H", $"WO_List".getItem(1))
      .withColumn("WO_09H", $"WO_List".getItem(2))
      .withColumn("WO_08H", $"WO_List".getItem(3))
      .withColumn("WO_07H", $"WO_List".getItem(4))
      .withColumn("WO_06H", $"WO_List".getItem(5))
      .withColumn("WO_05H", $"WO_List".getItem(6))
      .withColumn("WO_04H", $"WO_List".getItem(7))
      .withColumn("WO_03H", $"WO_List".getItem(8))
      .withColumn("WO_02H", $"WO_List".getItem(9))
      .withColumn("WO_01H", $"WO_List".getItem(10))
      .withColumn("WO_00H", $"WO_List".getItem(11))
      .withColumn("ORIGIN_AIRPORT_ID", $"FT".getItem("ORIGIN_AIRPORT_ID"))
      .withColumn("DEST_AIRPORT_ID", $"FT".getItem("DEST_AIRPORT_ID"))
      .withColumn("CRS_DEP_TIMESTAMP", $"FT".getItem("CRS_DEP_TIMESTAMP"))
      .withColumn("SCHEDULED_ARRIVAL_TIMESTAMP", $"FT".getItem("SCHEDULED_ARRIVAL_TIMESTAMP"))
      .withColumn("WD_11H", $"WD_List".getItem(0))
      .withColumn("WD_10H", $"WD_List".getItem(1))
      .withColumn("WD_09H", $"WD_List".getItem(2))
      .withColumn("WD_08H", $"WD_List".getItem(3))
      .withColumn("WD_07H", $"WD_List".getItem(4))
      .withColumn("WD_06H", $"WD_List".getItem(5))
      .withColumn("WD_05H", $"WD_List".getItem(6))
      .withColumn("WD_04H", $"WD_List".getItem(7))
      .withColumn("WD_03H", $"WD_List".getItem(8))
      .withColumn("WD_02H", $"WD_List".getItem(9))
      .withColumn("WD_01H", $"WD_List".getItem(10))
      .withColumn("WD_00H", $"WD_List".getItem(11))
      .withColumn("Class", $"FT".getItem("ARR_DELAY_NEW"))
      .drop("FT", "WO_List", "WD_List")
  }


  /**
   * Method for splitting the data into chunks and saving it as Parquet
   * @param df
   */
  def splitAndSaveData(df: DataFrame): Unit = {
    val fractions = Array.fill(20)(0.05)
    val splits = df.randomSplit(fractions)
    splits.zipWithIndex.foreach { case (split, index) =>
      split.write.format("parquet")
        .option("header", "true")
        .mode("overwrite")
        .save(s"$joinedTablePath/chunk_$index.parquet")
    }
  }

  /**
   *  Main method to execute the pipeline
   */
  def executePipeline(): Unit = {
    //create datasets
    val weather_ds: Dataset[Weather] = weather_df.as[Weather]
    val flight_ds: Dataset[Flight] = flight_df.as[Flight]

    val start: Long = System.currentTimeMillis() / 1000

    val flightOrigin = createFlightsOriginDataset(flight_ds)
    val WeatherOrigin = createWeatherOriginDataset(weather_ds)
    val flightOriginPartition = createFlightOriginPartition(flightOrigin, NumberOfPartitions)
    val weatherOriginPartition = createWeatherOriginPartition(WeatherOrigin, NumberOfPartitions)
    val joinedWeatherOriginAndFlightOrigin = joinWeatherOriginAndFlightOrigin(weatherOriginPartition, flightOriginPartition)

    println("1st Partitioning and Join OK")

    val aggregatedFOT = aggregateFOT(joinedWeatherOriginAndFlightOrigin)
    val flightDestination = createFlightDestination(aggregatedFOT)
    val joinedFlightsAndWeatherFinal = joinFlightsAndWeatherFinal(flightDestination, weatherOriginPartition ,NumberOfPartitions)

    println("2nd partitioning and Join OK")

    val processedFlightWeatherTable = ProcessFlightWeatherTable(joinedFlightsAndWeatherFinal)

    val end: Long = System.currentTimeMillis() / 1000
    val duration = end - start
    println("Duration of join: ", duration)

    splitAndSaveData(processedFlightWeatherTable)
    println("Data write complete")
  }

}
