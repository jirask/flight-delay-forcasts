# Flight Delay Forecasts: Developing a Predictive System Using Weather and Traffic Data

## Introduction
This project addresses the challenge of flight delays, a significant concern for the aviation industry causing economic losses and inconvenience. By leveraging data processing with Spark and machine learning models, we aim to predict flight delays, particularly those induced by weather conditions. Our goal is to enhance airlines' operations, improve passenger communication, and contribute to the industry's sustainability.

## Project Overview
- **Data Analysis and Preprocessing**: We processed flight and weather observation data, focusing on key attributes influencing delays.
- **Data Transformation**: The transformation phase involved merging preprocessed flight and weather data, feature selection, and target variable engineering.
- **Model Development**: We employed a Random Forest model for binary classification of flights as delayed or on-time.
- **Application Development**: The project features two primary applications: `dataprocessingapp` for data preprocessing and `delaypredictionapp` for applying the machine learning model.

## Repository Structure
- `dataprocessingapp`: Handles data cleaning and transformations.
- `delaypredictionapp`: Applies the Random Forest model and generates predictions.

## Running the Applications
### Data Processing Application
```bash
spark-submit \
--executor-cores 12 \
--num-executors 4 \
--executor-memory 8G \
--class com.DataProcessingApplication \
--master yarn \
dataprocessingapp_2.12-0.1.jar
```
### Delay Prediction Application
```bash
spark-submit \
--class com.DelayPredictionApplication \
--master yarn \
--num-executors 10 \
--executor-cores 5 \
--executor-memory 8G \
--driver-memory 4G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.executor.memoryOverhead=2G \
delaypredictionapp_2.12-0.1.jar
```
### Tips
If you are using the LAMSADE cluster: 
- If you do not have the raw data, you can enter this as your root path when running the data-processing-app : ```/students/iasd_20222023/szanutto/ProjectFlight```
- If you do not have processed data yet and wish to test the delay-prediction-app, you can find already processed data here: ```/students/iasd_20222023/asakhraoui/output ```

### Conclusion and Future Work
Our study indicates that while weather information improves delay predictions, the improvement is not significantly different from models without weather data. Future developments may include addressing data limitations, computational resources, and optimizing weather attribute selection for the model.



