lazy val commonSettings = Seq(
  name := "gdc-main",
  version := "0.1.0-SNAPSHOT",
  organization := "org.apache.spark.generic-downloader-connector",
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
  autoScalaLibrary := true
)

lazy val `gdc-core` = (project in file("gdc-core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.codehaus.plexus" % "plexus-archiver" % "2.2",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.mockito" % "mockito-core" % "1.10.19" % "test"),
    name := "gdc-core",
    spName := "alvsanand/gdc-core",
    sparkVersion := sys.props.get("spark.testVersion").getOrElse("1.5.2"),
    sparkComponents := Seq("core", "streaming")
  ).dependsOn()

lazy val `gdc-google` = (project in file("gdc-google")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "com.google.api-client" % "google-api-client-java6" % "1.22.0",
      "com.google.apis" % "google-api-services-storage" % "v1-rev86-1.22.0",
      "com.google.http-client" % "google-http-client-jackson2" % "1.22.0",
      "com.google.oauth-client" % "google-oauth-client-jetty" % "1.22.0",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.mockito" % "mockito-core" % "1.10.19" % "test"),
    name := "gdc-google",
    spName := "alvsanand/gdc-google"
  ).dependsOn(`gdc-core`)

lazy val root = (project in file(".")).
  aggregate(`gdc-core`, `gdc-google`).
  settings(
    aggregate in update := false
  )

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

parallelExecution in ThisBuild := false

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

//publishTo := {
//  val nexus = "https://oss.sonatype.org/"
//  if (version.value.endsWith("SNAPSHOT")) {
//    Some("snapshots" at nexus + "content/repositories/snapshots")
//  }
//  else {
//    Some("releases" at nexus + "service/local/staging/deploy/maven2")
//  }
//}

pomExtra := (
  <url>https://github.com/alvsanand/spark-generic-downloader-connector</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:alvsanand/spark-generic-downloader-connector.git</url>
      <connection>scm:git:git@github.com:alvsanand/spark-generic-downloader-connector.git</connection>
    </scm>
    <developers>
      <developer>
        <id>alvsanand</id>
        <name>Alvaro Santos Andres</name>
      </developer>
    </developers>)
