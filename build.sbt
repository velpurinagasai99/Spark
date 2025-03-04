//ThisBuild / version := "0.1.0-SNAPSHOT"
//ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Practice",
    scalaVersion := "2.12.0"
  )

//********************************  Compatible Versions are very important  **********************************//

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"