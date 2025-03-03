//ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Practice",
    scalaVersion := "2.11.11"
  )

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.5.0"
//  "org.apache.spark" %% "spark-sql" % "2.3.0"
//)

libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.1.0")
// https://mvnrepository.com/artifact/org.scala-sbt/sbt
libraryDependencies += "org.scala-sbt" % "sbt" % "2.0.0-M3" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3" % "provided"
