ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Practice"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
