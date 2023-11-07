package com.preprocess.weather

import com.Utils.SparkSessionWrapper
import com.twitter.chill.Input
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, collect_list, lit, mean, to_date, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * This class processes the weather table by: cleaning NA values: switching with the average or removing them, and keeping only columns of interest
 * @param rawWeatherData this variable con
 * @param timezoneData
 * @param airportList
 */
class WeatherPreprocessing(private val rawWeatherData: DataFrame, private val timezoneData: DataFrame, private val airportList: DataFrame) {

  private val spark: SparkSession = SparkSessionWrapper.spark
  import spark.implicits._

  private var processedWeatherData: DataFrame = _

  private def selectUsefulColumns(data: DataFrame, columns: Array[String]): DataFrame = {
    data.select(columns.map(col): _*)
  }

  private def filterWBANWeather(): DataFrame = {
    val validWBANs = airportList.select("F_WBAN").as[Int].collect().toSeq
    val usefulColumns = Array("WBAN", "Date", "Time", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindSpeed", "WeatherType", "HourlyPrecip")
    selectUsefulColumns(rawWeatherData, usefulColumns)
      .where($"WBAN".isin(validWBANs: _*))
  }

  private def extractSkyCondition(inputString: String): String = {
    Option(inputString) match {
      case Some(str) if str.nonEmpty =>
        print(str)
        val cloudLayers = str.trim.split("\\s+")
        val lastCloudLayer = cloudLayers.lastOption.getOrElse("OTHER")
        val pattern = "[A-Za-z]+(?=\\d*$)".r
        val result = pattern.findFirstIn(lastCloudLayer)
        result.getOrElse("OTHER")
      case _ => "M"
    }
  }

  private def extractWeatherTypes(input: String): String = {
    val weatherTypesList = List("RA", "SN", "FG", "FG+", "WIND", "FZDZ", "FZRA", "MIFG", "FZFG")
    val defaultValue = "OTHER"
    if (weatherTypesList.contains(input)) {
      input
    }
    else {
      defaultValue
    }
  }


  private def calculateAverage(weatherDF: DataFrame): DataFrame = {
    weatherDF
      .filter($"WindSpeed" =!= -1 && !$"DryBulbCelsius".isNull && !$"WindSpeed".isNull && !$"HourlyPrecip".isNull && !$"Visibility".isNull)
      .groupBy("Date", "AIRPORT_ID")
      .agg(
        mean("DryBulbCelsius").as("average_temperature"),
        mean("WindSpeed").as("average_windspeed"),
        mean("Visibility").as("average_visibility"),
        mean("HourlyPrecip").as("average_hourlyprecip")
      )
      .withColumnRenamed("AIRPORT_ID", "AVERAGE_AIRPORT_ID")
      .withColumnRenamed("Date", "AVERAGE_Date")
  }

  private val extractWeatherTypes: UserDefinedFunction = udf(extractWeatherTypes _)
  private val extractSkyConditionUDF: UserDefinedFunction = udf(extractSkyCondition _)
  private def cleanData(weatherDF: DataFrame): DataFrame = {
    val dailyAverage = calculateAverage(weatherDF)

    val weatherWithAirports = weatherDF
      .join(airportList, weatherDF("WBAN") === airportList("F_WBAN"), "inner")
      .withColumn("Time_hh", (col("Time") / 100).cast("Int"))
      .withColumn("Time_mm", (col("Time") % 100).cast("Int"))
      .withColumn("DryBulbCelsius", col("DryBulbCelsius").cast("double"))
      .withColumn("SkyCondition", extractSkyConditionUDF(col("SkyCondition")))
      .withColumn("Visibility", col("Visibility").cast("double"))
      .withColumn("WindSpeed", when(col("WindSpeed") === "VR", -1).otherwise(col("WindSpeed")).cast("int"))
      .withColumn("WeatherType", extractWeatherTypes(col("WeatherType")))
      .withColumn("HourlyPrecip", when(col("HourlyPrecip") === "T", 0).otherwise(col("HourlyPrecip")).cast("double"))
      .dropDuplicates("AIRPORT_ID", "Date", "Time_hh", "Time_mm")


    val weatherWithAverages = weatherWithAirports
      .join(dailyAverage, weatherWithAirports("AIRPORT_ID") === dailyAverage("AVERAGE_AIRPORT_ID") && weatherWithAirports("Date") === dailyAverage("AVERAGE_Date"), "inner")
      .drop("AVERAGE_AIRPORT_ID", "AVERAGE_Date")
      .withColumn("DryBulbCelsius", when($"DryBulbCelsius".isNull, dailyAverage("average_temperature")).otherwise($"DryBulbCelsius"))
      .withColumn("Visibility", when($"Visibility".isNull, dailyAverage("average_visibility")).otherwise($"Visibility"))
      .withColumn("WindSpeed", when($"WindSpeed".isNull, dailyAverage("average_windspeed")).otherwise($"WindSpeed"))
      .withColumn("HourlyPrecip", when($"HourlyPrecip".isNull, dailyAverage("average_hourlyprecip")).otherwise($"HourlyPrecip"))

    val groupedWeather = weatherWithAverages
      .orderBy($"AIRPORT_ID", $"Date", $"Time_hh", $"Time_mm".asc)
      .groupBy("AIRPORT_ID", "Date", "Time_hh")
      .agg(collect_list("Time_mm").as("collection"))
      .withColumn("Time_mm", col("collection").getItem(0).cast("Int"))
      .drop("collection")
      .withColumnRenamed("AIRPORT_ID", "W_AIRPORT_ID")
      .withColumnRenamed("Date", "W_Date")
      .withColumnRenamed("Time_hh", "W_Time_hh")
      .withColumnRenamed("Time_mm", "W_Time_mm")

    val tableWeather = weatherWithAverages.join(groupedWeather,
        weatherWithAverages("AIRPORT_ID") === groupedWeather("W_AIRPORT_ID") &&
          weatherWithAverages("Date") === groupedWeather("W_Date") &&
          weatherWithAverages("Time_hh") === groupedWeather("W_Time_hh") &&
          weatherWithAverages("Time_mm") === groupedWeather("W_Time_mm"),
        "inner")
      .drop("W_AIRPORT_ID", "W_Date", "W_Time_hh", "W_Time_mm")
      .withColumn("Date", to_date($"Date".cast("string"), "yyyyMMdd"))
      .withColumn("Weather_TIMESTAMP", functions.concat(col("Date").cast("string"), lit(" "), col("Time_hh").cast("string"), lit(":"), lit(0)).cast("timestamp"))
      .drop("Date", "Time", "Time_hh", "Time_mm")
      .select(Array("AIRPORT_ID", "Weather_TIMESTAMP", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindSpeed", "WeatherType", "HourlyPrecip").map(col): _*)

    tableWeather
  }

  private def processData(): DataFrame = {
    val filteredWeather = filterWBANWeather()
    cleanData(filteredWeather)
  }

  /**
   * builds the processed weather table
   * @return processedWeatherData
   */

  def buildWeatherTable(): DataFrame = {
    this.processedWeatherData = processData()
    this.processedWeatherData
  }

  /**
   * Getter for the processed weather data.
   */
  def getProcessedWeatherData: DataFrame = this.processedWeatherData

}