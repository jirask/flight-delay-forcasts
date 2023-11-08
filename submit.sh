# Variables
  MAX_WAIT_TIME=3600                # Maximum wait time in seconds
  SLEEP_INTERVAL=30                # How long to wait between checks in seconds
  DATA_PROCESSING_JAR="target/scala-2.12/data_processing.jar"  # Replace with the path to your DataProcessingApplication jar
  DELAY_PREDICTION_JAR="target/scala-2.12/delay_prediction.jar"  # Replace with the path to your DelayPredictionApplication jar

  # Prompt the user to enter the HDFS path where DataProcessingApplication will write data
  echo "Enter the HDFS path where DataProcessingApplication writes data:"
  read -r DATA_PATH

  # Ensure that DATA_PATH is not empty
  if [[ -z "$DATA_PATH" ]]; then
    echo "You must enter a valid HDFS path."
    exit 1
  fi
  # Run the DataProcessingApplication
  spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --executor-cores 2 \
    --num-executors 1 \
    --class com.DataProcessingApplication \
    $DATA_PROCESSING_JAR

  # Wait for the data to appear in HDFS
  elapsed_time=0
  while ! hdfs dfs -test -e $DATA_PATH
  do
    sleep $SLEEP_INTERVAL
    elapsed_time=$(($elapsed_time + $SLEEP_INTERVAL))

    if [ $elapsed_time -ge $MAX_WAIT_TIME ]; then
      echo "Data not found in HDFS after waiting for $MAX_WAIT_TIME seconds. Exiting."
      exit 1
    fi

    echo "Waiting for data to be written to HDFS..."
  done

  echo "Data is available in HDFS. Proceeding to run DelayPredictionApplication."

  # Run the DelayPredictionApplication
  spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --executor-cores 2 \
    --num-executors 1 \
    --class com.DelayPredictionApplication \
    $DELAY_PREDICTION_JAR

