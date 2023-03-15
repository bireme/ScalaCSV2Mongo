ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "csv2mongodb"
  )

val mongoVersion = "4.9.0"
val commonsCVSVersion = "1.10.0"
val json4sVersion = "4.1.0-M1"
val mUnit = "1.0.0-M7"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion,
  "org.apache.commons" % "commons-csv" % commonsCVSVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.scalameta" %% "munit" % mUnit % Test
)