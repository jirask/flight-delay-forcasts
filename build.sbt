name := "FlightsProjects"
version := "0.1"
scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.3.2" % "provided",
  "io.delta" %% "delta-core" % "2.2.0",
  "org.slf4j" % "slf4j-api" % "2.0.5"
)
