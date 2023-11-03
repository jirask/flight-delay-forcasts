package com.preprocess.weather

case class Weather
(
  AIRPORT_ID: Int,
  Weather_TIMESTAMP: java.sql.Timestamp,
  DryBulbCelsius: Double,
  SkyCondition: String,
  Visibility: Double,
  WindDirection: Int,
  WindSpeed: Double,
  WeatherType: String,
  StationPressure: Double
)
