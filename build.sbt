
//ThisBuild / version := "0.1.0-SNAPSHOT"
//ThisBuild / scalaVersion := "2.13.0"
mainClass in Compile := Some("EMRFiles.OrderStatusEMR")     //While building the executable Jar, give your main class here
lazy val root = (project in file("."))
  .settings(
    name := "Scala_Practice"
    ,scalaVersion := "2.12.18"
    //,scalaVersion := "2.11.8"       //Use these when running spark version -1.0 code...like when using spark contest
  )

//********************************  Compatible Versions are very important  **********************************//

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.4.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.4.0" % "provided"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8" //Use these when running scala version -1.0 code...like when using spark contest


