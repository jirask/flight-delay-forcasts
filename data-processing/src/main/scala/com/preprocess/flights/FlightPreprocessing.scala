package com.preprocess.flights

import com.Utils.SparkSessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class processes the flight table by: cleaning NA values and keeping only columns of interest
 * @param rawFlightData
 * @param timezoneData
 */
class FlightPreprocessing(private val rawFlightData: DataFrame, private val timezoneData: DataFrame) {

  private val spark: SparkSession = SparkSessionWrapper.spark
  import spark.implicits._

  private var processedFlightData: DataFrame = _
  private var airportList: DataFrame = _

  /**
   * Cleans the flight data to remove unwanted records and values.
   */
  def cleanData(): DataFrame = {
    rawFlightData
      .na.fill(0, Seq("WEATHER_DELAY", "NAS_DELAY", "ARR_DELAY_NEW"))
      .na.drop(Seq("CRS_ELAPSED_TIME"))
      .filter(!($"DIVERTED" === 1 || $"CANCELLED" === 1))
      .drop("DIVERTED", "CANCELLED", "_C12")
      .dropDuplicates()
      .join(timezoneData, $"ORIGIN_AIRPORT_ID" === timezoneData("AirportID"), "inner")
      .withColumnRenamed("TimeZone", "TZone_ORIGIN_AIRPORT_ID")
      .withColumnRenamed("WBAN", "ORIGIN_WBAN")
      .drop("AirportID")
      .join(timezoneData, $"DEST_AIRPORT_ID" === timezoneData("AirportID"), "inner")
      .withColumnRenamed("TimeZone", "TZone_DEST_AIRPORT_ID")
      .withColumnRenamed("WBAN", "DEST_WBAN")
      .drop("AirportID")
      .withColumn("Dest-Orig", $"TZone_DEST_AIRPORT_ID" - $"TZone_ORIGIN_AIRPORT_ID")
      .drop("TZone_DEST_AIRPORT_ID", "TZone_ORIGIN_AIRPORT_ID")
  }

  /**
   * Processes cleaned data to convert time values and select necessary columns.
   */
  def processData(cleanedData: DataFrame): DataFrame = {
    val minutesToSecondsUDF = udf((minutes: Double) => (minutes * 60).toLong)
    val hoursToSecondsUDF = udf((hours: Double) => (hours * 3600).toLong)

    cleanedData
      .withColumn("FL_DATE", date_format($"FL_DATE", "yyyy-MM-dd"))
      .withColumn("CRS_DEP_TIME_string", from_unixtime(unix_timestamp(format_string("%04d", $"CRS_DEP_TIME"), "HHmm"), "HH:mm"))
      .withColumn("DATE_CRS_DEP_TIME_string", from_unixtime(unix_timestamp(concat($"FL_DATE".cast("string"), lit(" "), $"CRS_DEP_TIME_string"), "yyyy-MM-dd HH:mm")))
      .withColumn("CRS_DEP_TIMESTAMP", to_timestamp($"DATE_CRS_DEP_TIME_string"))
      .withColumn("SCHEDULED_ARRIVAL_TIMESTAMP_string", from_unixtime(unix_timestamp($"DATE_CRS_DEP_TIME_string") + minutesToSecondsUDF($"CRS_ELAPSED_TIME") + hoursToSecondsUDF($"Dest-Orig")))
      .withColumn("SCHEDULED_ARRIVAL_TIMESTAMP", to_timestamp($"SCHEDULED_ARRIVAL_TIMESTAMP_string"))
      .drop("CRS_DEP_TIME_string", "DATE_CRS_DEP_TIME_string", "SCHEDULED_ARRIVAL_TIMESTAMP_string", "ORIGIN_WBAN", "DEST_WBAN")
      .select("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIMESTAMP", "SCHEDULED_ARRIVAL_TIMESTAMP", "ARR_DELAY_NEW")
  }

  /**
   * Extracts unique WBAN values for both origin and destination airports
   ** @return A list of all airports
   */
  def buildAirportList(): DataFrame = {

    def extractAirportInfo(columnNames: (String, String)): DataFrame =
      processedFlightData.select(columnNames._1, columnNames._2)
        .withColumnRenamed(columnNames._1, "AIRPORT_ID")
        .withColumnRenamed(columnNames._2, "F_WBAN")
        .distinct

    val destination = extractAirportInfo(("DEST_AIRPORT_ID", "DEST_WBAN"))
    val origin = extractAirportInfo(("ORIGIN_AIRPORT_ID", "ORIGIN_WBAN"))

    destination.union(origin).distinct
  }


  /**
   * Builds the final flight table.
   */
  def buildFlightTable(): DataFrame = {
    val cleanedData = cleanData()
    processedFlightData = processData(cleanedData)
    airportList = buildAirportList()
    processedFlightData
  }

  /**
   * Getter for the processed flight data.
   */
  def getProcessedFlightData: DataFrame = processedFlightData

  /**
   * Get the airport List
   */
  def getAirportList: DataFrame = airportList
}
