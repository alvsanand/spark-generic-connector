
lazy val commonSettings = Seq(
  name := "gdc-main",
  version := "0.1.0-SNAPSHOT",
  organization := "org.apache.spark.generic-downloader-connector",
  scalaVersion := "2.11.7",
  spName := "generic-downloader-connector",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
  sparkVersion := sys.props.get("spark.testVersion").getOrElse("1.5.2")
)

lazy val `gdc-core` = (project in file("gdc-core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "compile",
      "org.apache.spark" %% "spark-streaming" % sparkVersion.value % "compile",
      "org.codehaus.plexus" % "plexus-archiver" % "2.2",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.mockito" % "mockito-core" % "1.10.19" % "test"),
    name := "gdc-core"
  ).dependsOn()

lazy val `gdc-google` = (project in file("gdc-google")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
      "com.google.api-client" % "google-api-client-java6" % "1.22.0",
      "com.google.apis" % "google-api-services-storage" % "v1-rev86-1.22.0",
      "com.google.http-client" % "google-http-client-jackson2" % "1.22.0",
      "com.google.oauth-client" % "google-oauth-client-jetty" % "1.22.0",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.mockito" % "mockito-core" % "1.10.19" % "test"),
    name := "gdc-google"
  ).dependsOn(`gdc-core`)

lazy val root = (project in file(".")).
  aggregate(`gdc-core`, `gdc-google`).
  settings(
    aggregate in update := false
  )

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

parallelExecution in ThisBuild := false

