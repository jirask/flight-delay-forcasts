package com.preprocess.weather

case class Weather
(
  AIRPORT_ID: Int,
  Weather_TIMESTAMP: java.sql.Timestamp,
  DryBulbCelsius: Double,
  SkyCOndition: String,
  Visibility: Double,
  WindSpeed: Double,
  WeatherType: String
)
