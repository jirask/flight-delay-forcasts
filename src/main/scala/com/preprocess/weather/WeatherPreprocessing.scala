package com.preprocess.weather

import com.Utils.SparkSessionWrapper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, collect_list, lit, to_date, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class WeatherPreprocessing(private val rawWeatherData: DataFrame, private val timezoneData: DataFrame, private val airportList: DataFrame) {

  private val spark: SparkSession = SparkSessionWrapper.spark
  import spark.implicits._

  private var processedWeatherData: DataFrame = _

  private def selectUsefulColumns(data: DataFrame, columns: Array[String]): DataFrame = {
    data.select(columns.map(col): _*)
  }

  private def filterWBANWeather(): DataFrame = {
    val validWBANs = airportList.select("F_WBAN").as[Int].collect().toSeq
    val usefulColumns = Array("WBAN", "Date", "Time", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindDirection", "WindSpeed", "WeatherType", "StationPressure")
    selectUsefulColumns(rawWeatherData, usefulColumns)
      .where($"WBAN".isin(validWBANs: _*))
  }

  private def extractSkyCondition(inputString: String): String = {
    val cloudLayers = inputString.split(" ")
    val lastCloudLayer = cloudLayers.lastOption.getOrElse("")
    "[A-Za-z]+$".r.findFirstIn(lastCloudLayer).getOrElse("")
  }


  private def cleanData(weatherDF: DataFrame): DataFrame = {
    val extractSkyConditionUDF: UserDefinedFunction = udf(extractSkyCondition _)

    val joinedWeather = weatherDF
      .join(airportList, weatherDF("WBAN") === airportList("F_WBAN"), "inner")
      .withColumn("Time_hh", (col("Time") / 100).cast("Int"))
      .withColumn("Time_mm", (col("Time") % 100).cast("Int"))
      .withColumn("DryBulbCelsius", col("DryBulbCelsius").cast("double"))
      .withColumn("SkyCondition", when(col("SkyCondition") === "M", "").otherwise(extractSkyConditionUDF(col("SkyCondition"))))
      .withColumn("Visibility", col("Visibility").cast("double"))
      .withColumn("WindSpeed", when(col("WindSpeed") === "VR", -1).otherwise(col("WindSpeed")).cast("int"))
      .withColumn("WeatherType", when(col("WeatherType") === "M", "").otherwise(col("WeatherType")))
      .dropDuplicates("AIRPORT_ID", "Date", "Time_hh", "Time_mm")

    val groupedWeather = joinedWeather
      .orderBy($"AIRPORT_ID", $"Date", $"Time_hh", $"Time_mm".asc)
      .groupBy("AIRPORT_ID", "Date", "Time_hh")
      .agg(collect_list("Time_mm").as("collection"))
      .withColumn("Time_mm", col("collection").getItem(0).cast("Int"))
      .drop("collection")
      .withColumnRenamed("AIRPORT_ID", "W_AIRPORT_ID")
      .withColumnRenamed("Date", "W_Date")
      .withColumnRenamed("Time_hh", "W_Time_hh")
      .withColumnRenamed("Time_mm", "W_Time_mm")

    val tableWeather = joinedWeather.join(groupedWeather,
        joinedWeather("AIRPORT_ID") === groupedWeather("W_AIRPORT_ID") &&
          joinedWeather("Date") === groupedWeather("W_Date") &&
          joinedWeather("Time_hh") === groupedWeather("W_Time_hh") &&
          joinedWeather("Time_mm") === groupedWeather("W_Time_mm"),
        "inner")
      .drop("W_AIRPORT_ID", "W_Date", "W_Time_hh", "W_Time_mm")
      .withColumn("Date", to_date($"Date".cast("string"), "yyyyMMdd"))
      .withColumn("Weather_TIMESTAMP", functions.concat(col("Date").cast("string"), lit(" "), col("Time_hh").cast("string"), lit(":"), lit(0)).cast("timestamp"))
      .drop("Date", "Time", "Time_hh", "Time_mm")
      .select(Array("AIRPORT_ID", "Weather_TIMESTAMP", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindDirection", "WindSpeed", "WeatherType", "StationPressure").map(col): _*)

    tableWeather
  }

  private def processData(): DataFrame = {
    val filteredWeather = filterWBANWeather()
    cleanData(filteredWeather)
  }

  def buildWeatherTable(): DataFrame = {
    this.processedWeatherData = processData()
    this.processedWeatherData
  }

  def getProcessedWeatherData: DataFrame = this.processedWeatherData

}