name := "ScalaCSV2Mongo"
version := "0.1.0"
organization := "bireme"
scalaVersion := "2.13.10"

publishMavenStyle := true

val mongoVersion = "4.9.0"
val commonsCVSVersion = "1.10.0"
val json4sVersion = "4.1.0-M1"
val mUnit = "1.0.0-M7"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion,
  "org.apache.commons" % "commons-csv" % commonsCVSVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.scalameta" %% "munit" % mUnit % Test,
)

publishTo := Some("GitHub Package Registry" at "https://maven.pkg.github.com/bireme/ScalaCSV2Mongo")