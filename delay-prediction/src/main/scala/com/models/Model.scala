package com.models

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.matching.Regex
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}

import scala.collection.mutable.ArrayBuffer

class Model(pathDataML: String, delayThreshold: Int,
            originTimeRange: Int, destinationTimeRange: Int, maxCat: Int,
            handleInvalid: String, numTreesForRandomForest: Int)
{
  @transient val spark: SparkSession = SparkSession.builder()
    .appName("FlightsDelayPredictionML")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  private def readChunk(index: Int): DataFrame = {
    spark.read.format("parquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(s"$pathDataML/chunk_$index.parquet")
  }

  /**
   * Read all chunks and union them into one DataFrame
   **/
  def readAllChunks(): DataFrame = {
    val chunks = (0 until 10).map(readChunk)
    chunks.reduce(_ union _)
  }

  /**
   * Creation of target depending on threshold, year, month and date columns, cast airport_id to string
   */
  def processTable(df: DataFrame): DataFrame = {
    df.withColumn("Target", when($"Class" >= delayThreshold, 1).otherwise(0))
      .withColumn("ORIGIN_AIRPORT_ID", $"ORIGIN_AIRPORT_ID".cast("string"))
      .withColumn("DEST_AIRPORT_ID", $"DEST_AIRPORT_ID".cast("string"))
      .withColumn("Hour_SO", date_format($"CRS_DEP_TIMESTAMP", "HH").cast("int"))
      .withColumn("Hour_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "HH").cast("int"))
      .withColumn("Day_SO", date_format($"CRS_DEP_TIMESTAMP", "F").cast("int"))
      .withColumn("Day_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "F").cast("int"))
      .withColumn("Month_SO", date_format($"CRS_DEP_TIMESTAMP", "M").cast("int"))
      .withColumn("Month_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "M").cast("int"))
      .withColumn("Year_SO", date_format($"CRS_DEP_TIMESTAMP", "y").cast("int"))
      .withColumn("Year_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "y").cast("int"))
      .drop("Class", "CRS_DEP_TIMESTAMP", "SCHEDULED_ARRIVAL_TIMESTAMP")

    //      .withColumn("Hour_SO", hour($"CRS_DEP_TIMESTAMP"))
//      .withColumn("Hour_SA", hour($"SCHEDULED_ARRIVAL_TIMESTAMP"))
//      .withColumn("Day_SO", dayofweek($"CRS_DEP_TIMESTAMP"))
//      .withColumn("Day_SA", dayofweek($"SCHEDULED_ARRIVAL_TIMESTAMP"))
//      .withColumn("Month_SO", month($"CRS_DEP_TIMESTAMP"))
//      .withColumn("Month_SA", month($"SCHEDULED_ARRIVAL_TIMESTAMP"))
//      .withColumn("Year_SO", year($"CRS_DEP_TIMESTAMP"))
//      .withColumn("Year_SA", year($"SCHEDULED_ARRIVAL_TIMESTAMP"))
//      .drop("Class", "CRS_DEP_TIMESTAMP", "SCHEDULED_ARRIVAL_TIMESTAMP")
  }

  /**
   * Filter columns based on a pattern and a threshold
   */
  private def filterColumnsByPattern(df: DataFrame, pattern: String, threshold: Int): Array[String] = {
    val numberPattern: Regex = "\\d+".r
    df.columns.filter(_.contains(pattern)).flatMap { colName =>
      numberPattern.findAllIn(colName).toList match {
        case Nil => None
        case numbers => if (numbers.map(_.toInt).exists(_ <= threshold)) Some(colName) else None
      }
    }
  }

  /**
   * Prepare the final table for machine learning
   */
  def prepareFinalTable(df: DataFrame): DataFrame = {
    val constantColumns = Array("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "Hour_SO", "Hour_SA",
      "Month_SO", "Month_SA", "Day_SO", "Day_SA", "Year_SO", "Year_SA", "Target")
    val originTimeRangeCol = filterColumnsByPattern(df, "WO", originTimeRange)
    val destinationTimeRangeCol = filterColumnsByPattern(df, "WD", destinationTimeRange)

    df.select((originTimeRangeCol ++ destinationTimeRangeCol ++ constantColumns).map(col): _*)
  }

  /**
   * Split struct columns and prepare final DataFrame for machine learning
   */
  def splitStructColumns(df: DataFrame): DataFrame = {
    // Define a map of index to field names
    val indexToFieldMap: Map[Int, String] = Map(
      0 -> "AIRPORT_ID",
      1 -> "Weather_TIMESTAMP",
      2 -> "DryBulbCelsius",
      3 -> "SkyCondition",
      4 -> "Visibility",
      5 -> "WindSpeed",
      6 -> "WeatherType",
      7 -> "HourlyPrecip"
    )

    // Filter out columns starting with 'W' and prepare for splitting
    val columnsToApplySplit = df.columns.filter(_.startsWith("W")).toSeq
    val columnsToDrop = df.columns.filter(_.startsWith("W")).toList
    val structSize = 8 // Assuming each struct has 8 components

    // Split each struct into individual columns
    columnsToApplySplit.foldLeft(df) { (tempDs, colName) =>
      val colNameIndices = (0 until structSize).map(index => (index, s"${index}_$colName"))

      val updatedInnerDs = colNameIndices.foldLeft(tempDs) { (innerTempDs, indexAndNewColumnName) =>
        val (index, newColumnName) = indexAndNewColumnName
        val fieldName = indexToFieldMap(index) // Get the field name from the map
        val colExpr = col(s"$colName.$fieldName").alias(newColumnName)
        innerTempDs.withColumn(newColumnName, colExpr)
      }

      updatedInnerDs
    }.drop(columnsToDrop: _*) // Drop the original struct columns
  }

  // Function to remove duplicate airport ID columns
  def removeDuplicateAirportIds(df: DataFrame): DataFrame = {
    df.select(df.columns.filter(!_.contains("0_")).map(col): _*)
  }

  // Function to split timestamp columns into year, month, day, and hour
  def splitTimestampColumns(df: DataFrame): DataFrame = {
    val timeStampCol = df.dtypes.filter(_._2 == "TimestampType").map(_._1)
    timeStampCol.foldLeft(df) { (tempDs, colName) =>
      tempDs
        .withColumn(s"Year_$colName", year(col(colName)))
        .withColumn(s"Month_$colName", month(col(colName)))
        .withColumn(s"Day_$colName", dayofweek(col(colName)))
        .withColumn(s"Hour_$colName", hour(col(colName)))
    }.drop(timeStampCol: _*)
  }

  /**
   * Takes a DataFrame and a list of weather types and creates new columns in the DataFrame
   * for each weather type. Columns that contain "6_W" are considered for splitting.
   *
   * @param weatherDf The DataFrame containing weather data.
   * @return A DataFrame with new weather type columns.
   */
  def createWeatherTypeColumns(weatherDf: DataFrame): DataFrame = {
    // Relevant weather types that might affect the delay
    val weatherTypes = List("RA","SN","FG+","WIND","FZDZ","FZRA","FZFG")
    // Select columns to split based on column names starting with "6_W"
    val columnsWeatherTypeToApplySplit = weatherDf.columns.filter(_.startsWith("6_W")).toSeq

    var df = weatherDf

    // Add new columns for each weather type
    weatherTypes.foreach { weatherType =>
      columnsWeatherTypeToApplySplit.foreach { colName =>
        df = df.withColumn(s"${colName}_$weatherType", when(col(colName).contains(weatherType), 1).otherwise(0))
      }
    }

    // Drop the original weather type columns
    df.drop(columnsWeatherTypeToApplySplit: _*)
  }

  // Function to select columns based on user choices regarding year, month, and day
  def selectColumnsBasedOnUserChoices(df: DataFrame, year: Int, month: Int, day: Int): DataFrame = {
    val colSansDate = df.columns.filter { col =>
      (year != 0 || !col.contains("Year")) &&
        (month != 0 || !col.contains("Month")) &&
        (day != 0 || !col.contains("Day"))
    }
    df.select(colSansDate.map(col): _*)
  }

  // Function to prepare the transformation pipeline
  def autoPipelineReg(textCols: Array[String], numericCols: Array[String], maxCat: Int, handleInvalid: String): Pipeline = {
    val prefix = "indexed_"
    val featuresVec = "featuresVec"
    val featuresVecIndex = "features"
    var featureIndices = Array(("", ""))
    // StringIndexer
    val outAttsNames = textCols.map(prefix + _)
    val stringIndexer = new StringIndexer()
      .setInputCols(textCols)
      .setOutputCols(outAttsNames)
      .setHandleInvalid(handleInvalid)

    // VectorAssembler
    val features = outAttsNames ++ numericCols
    featureIndices = features.zipWithIndex.map { case (f, i) => ("feature " + i, f) }

    val vectorAssembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol(featuresVec)
      .setHandleInvalid(handleInvalid)

    // VectorIndexer
    val vectorIndexer = new VectorIndexer()
      .setInputCol(featuresVec)
      .setOutputCol(featuresVecIndex)
      .setMaxCategories(maxCat)
      .setHandleInvalid(handleInvalid)

    // Pipeline
    new Pipeline().setStages(Array(stringIndexer, vectorAssembler, vectorIndexer))
  }

  // Method for data partitioning
  def partitionData(df: DataFrame, numPartitions: Int): DataFrame = {
    df.repartition(numPartitions, col("ORIGIN_AIRPORT_ID"), col("DEST_AIRPORT_ID"))
  }

  // Method for balancing the dataset
  def balanceDataset(df: DataFrame): DataFrame = {
    val labelCounts = df.groupBy("label").count().collect()
    val maxCount = labelCounts.map(_.getLong(1)).max
    val minCount = labelCounts.map(_.getLong(1)).min
    val balanceRatio = minCount.toDouble / maxCount.toDouble
    val fractions = Map(1 -> 1.0, 0 -> balanceRatio)
    df.stat.sampleBy("label", fractions, seed = 42)
  }

//  // Method for Random Forest classifier configuration and model fitting
//  def trainRandomForestClassifier(trainingData: DataFrame): RandomForestClassifier = {
//    new RandomForestClassifier()
//      .setLabelCol("label")
//      .setFeaturesCol("features")
//      .setMaxBins(maxCat)
//      .setNumTrees(numTreesForRandomForest)
//      .setMaxDepth(20)
//      .setSubsamplingRate(0.5)
//      .setMaxBins(maxCat)
//      .setMinInstancesPerNode(20)
//  }


  // Method for evaluating the model
  def evaluateModel(predictions: DataFrame): Unit = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(predictions)

    val tp = predictions.filter($"prediction" === 1 && $"label" === 1).count().toDouble
    val fp = predictions.filter($"prediction" === 1 && $"label" === 0).count().toDouble
    val fn = predictions.filter($"prediction" === 0 && $"label" === 1).count().toDouble

    val precision = if (tp + fp > 0) tp / (tp + fp) else 0
    val recall = if (tp + fn > 0) tp / (tp + fn) else 0
    val f1score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0

    println(s"AUC: $auc")
    println(s"Precision: $precision")
    println(s"Recall: $recall")
    println(s"F1 Score: $f1score")
  }

  // Execute the complete data processing and machine learning pipeline
  def executePipeline(): Unit = {
    // Read and process data
    val allChunks = readAllChunks()
    println("Step 1: Data loading OK")

    val processedTable = processTable(allChunks)
    println("processedTable")
    processedTable.show(2)

    println("Step 2: Table with selected time slots")
    val finalTable = prepareFinalTable(processedTable)
    println("finalTable")
    finalTable.show(2)

    println("Step 3: Split of weather tuples")
    val splitTable = splitStructColumns(finalTable)
    println("splitTable")
    splitTable.show(2)

    println("Step 4: Splitting of struct columns completed")

    val noDuplicateIdsTable = removeDuplicateAirportIds(splitTable)
    println("noDuplicateIdsTable")
    noDuplicateIdsTable.show(2)

    println("Step 5: Removal of duplicate airport IDs")

    val timestampSplitTable = splitTimestampColumns(noDuplicateIdsTable)
    println("timestampSplitTable")
    timestampSplitTable.show(2)

    println("Step 6: Splitting of timestamp columns")

    //Creates new columns in the DataFrame for the weather type
    val withWeatherTypeColumns = createWeatherTypeColumns(timestampSplitTable)
    println("withWeatherTypeColumns")
    withWeatherTypeColumns.show(2)


    // Example user choices: annee = 1, mois = 0, jour = 0
    val userChoiceFilteredTable = selectColumnsBasedOnUserChoices(withWeatherTypeColumns, year = 1, month = 0, day = 0)
    println("userChoiceFilteredTable")
    userChoiceFilteredTable.show(2)

    println("Step 7: Selection of columns based on user choices")

    // Extract column names by type for the final file
    val numericCols = userChoiceFilteredTable.dtypes
      .filter(tuple => tuple._2.equals("DoubleType") || tuple._2.equals("IntegerType"))
      .map(_._1).filterNot(_.contains("Target"))

    val textCols = userChoiceFilteredTable.dtypes
      .filter(_._2 == "StringType")
      .map(_._1)

    // Create the pipeline
    val pipeline = autoPipelineReg(textCols, numericCols, maxCat, handleInvalid)
    println("Step 8: Pipeline declaration")

    // Prepare and transform data with the pipeline
    val numPartitions = 12
    val dataWithPartitions = partitionData(userChoiceFilteredTable, numPartitions)
    val dataEnc = pipeline.fit(dataWithPartitions).transform(dataWithPartitions)
      .select(col("features"), col("Target"))
      .withColumnRenamed("Target", "label")
    val nbRows = dataEnc.count()

    println(s"Step 9: Balance the dataset to handle class imbalance;  $nbRows")

    // Balance the dataset to handle class imbalance
    val dataBalanced = balanceDataset(dataEnc)

    // Split the balanced dataset into training and test sets
    val Array(trainingData, testData) = dataBalanced.randomSplit(Array(0.7, 0.3))
    println("Step 10: training model")


    // Train the Random Forest classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(maxCat)
      .setNumTrees(numTreesForRandomForest)
      .setMaxDepth(20)
      .setSubsamplingRate(0.5)
      .setMaxBins(270)
      .setMinInstancesPerNode(20)

    val rfModel = rf.fit(trainingData)
    println("Step 11: Generate predictions on the test set using the trained Random Forest model")

    // Generate predictions on the test set using the trained Random Forest model
    val rfPredictions = rfModel.transform(testData)
    println("Step 12: Evaluate the model")

    // Evaluate the model
    evaluateModel(rfPredictions)
  }
}
