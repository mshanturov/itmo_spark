ThisBuild / scalaVersion := "2.13.16"

name := "lab7-data-mart"
version := "0.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "org.mongodb" % "mongodb-driver-sync" % "5.6.0",
  "com.typesafe" % "config" % "1.4.4",
  "com.lihaoyi" %% "ujson" % "4.3.2"
)

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
