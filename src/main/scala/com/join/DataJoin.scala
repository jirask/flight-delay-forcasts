package com.join

import com.Utils.SparkSessionWrapper
import com.preprocess.flights.{Flight, FlightPreprocessing}
import com.preprocess.weather.{Weather, WeatherPreprocessing}
import org.apache.spark.sql.functions._

import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.nio.file.Paths
import scala.io.StdIn
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession, functions}

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
          weather.WeatherType
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

  private def rowToWeather(row: Row): Weather = {
    Weather(
      AIRPORT_ID = row.getInt(0),
      Weather_TIMESTAMP = row.getTimestamp(1),
      DryBulbCelsius = row.getDouble(2),
      SkyCOndition = row.getString(3),
      Visibility = row.getDouble(4),
      WindSpeed = row.getDouble(5),
      WeatherType = row.getString(6),
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

  def FinalFOT(FOT_joined_Origin: DataFrame): DataFrame = {
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

  def createFinalTable(df: DataFrame): DataFrame = {
    df
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
    val fractions = Array(0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05)
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
    val weather_ds: Dataset[Weather] = flight_df.as[Weather]
    val flight_ds: Dataset[Flight] = flight_df.as[Flight]

    val start: Long = System.currentTimeMillis() / 1000

    val FT = createFlightsOriginDataset(flight_ds)
    val OT = createWeatherOriginDataset(weather_ds)

    val (repartitionedOT, repartitionedFTOrigin) = repartitionDataFrames(OT,FT,NumberOfPartitions)

    println("Partitioning OK")

//    val joinedFOT_origin = joinOTAndFT(repartitionedOT, repartitionedFTOrigin)
//    println("Join OK")
//    val aggregatedFOT = aggregateFOT(joinedFOT_origin)
//    val FlightTableDestination = createFTDestination(aggregatedFOT)
//    //val flightFinalOrigin = FinalFOT()
//
//    val FT_Destination_partition = FlightTableDestination.repartition(NumberOfPartitionsPartitions, $"FT_KEY".getItem("_1"))
//
//    println("2nd partitioning OK")
//
//    val processedDF = processDataFrame(joinedDF)
//    println("Processing OK")
//
//    val end: Long = System.currentTimeMillis() / 1000
//    val duration = end - start
//    println("Duration of join: ", duration)
//
//    splitAndSaveData(processedDF, Array.fill(20)(0.05))

    println("Data write complete")
  }

}
