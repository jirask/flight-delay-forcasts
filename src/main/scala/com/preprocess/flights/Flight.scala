package com.preprocess.flights

case class Flight
(
     ORIGIN_AIRPORT_ID: Int,
     DEST_AIRPORT_ID: Int,
     CRS_DEP_TIMESTAMP: java.sql.Timestamp,
     ACTUAL_DEPARTURE_TIMESTAMP: java.sql.Timestamp,
     SCHEDULED_ARRIVAL_TIMESTAMP: java.sql.Timestamp,
     ACTUAL_ARRIVAL_TIMESTAMP: java.sql.Timestamp,
     ARR_DELAY_NEW: Double
)

