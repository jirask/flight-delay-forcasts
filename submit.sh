spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --executor-cores 2 \
  --num-executors 1 \
  --class com.slim.FligthsDelayPrediction \
  target/scala-2.12/flightsprojects_2.12-0.1.jar
