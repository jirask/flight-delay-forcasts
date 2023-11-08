val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.12.13"
  // Add other settings that are common to both projects
)

// Define the DataProcessingApplication project
lazy val dataProcessingApplication = (project in file("data-processing"))
  .settings(
    commonSettings,
    name := "DataProcessingApp",
    mainClass in (Compile, run) := Some("com.app.DataProcessingApplication"),
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % "3.0.2",
          "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided",
          "org.apache.spark" %% "spark-mllib" % "3.0.2" % "provided",
          "io.delta" %% "delta-core" % "1.0.0",
          "org.slf4j" % "slf4j-api" % "1.7.25"
        )
  )

// Define the DelayPredictionApplication project
lazy val delayPredictionApplication = (project in file("delay-prediction"))
  .settings(
    commonSettings,
    name := "DelayPredictionApp",
    mainClass in (Compile, run) := Some("com.app.DelayPredictionApplication"),
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % "3.0.2",
          "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided",
          "org.apache.spark" %% "spark-mllib" % "3.0.2" % "provided",
          "io.delta" %% "delta-core" % "1.0.0",
          "org.slf4j" % "slf4j-api" % "1.7.25"
        )
  )

// Define the root project which aggregates the two applications
lazy val root = (project in file("."))
  .aggregate(dataProcessingApplication, delayPredictionApplication)
  .settings(
    publish := {},
    publishLocal := {}
  )

