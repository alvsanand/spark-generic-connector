
val testSparkVersion_1_x = settingKey[String]("The version of Spark to test against.")
val testSparkVersion_2_x = settingKey[String]("The version of Spark to test against.")

lazy val commonSettings = Seq(
  organization := "es.alvsanand",
  name := "gdc-main",
  version := "0.1.0-SNAPSHOT",

  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),

  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",

    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "org.mockito" % "mockito-core" % "1.10.19" % "test"),

  publishMavenStyle := true,

  spIgnoreProvided := true,

  spAppendScalaVersion := true,

  spIncludeMaven := true,

  parallelExecution in ThisBuild := false,

  // Skip tests during assembly
  test in assembly := {},

  ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
    if (scalaBinaryVersion.value == "2.10") false
    else true
  },
  // publishTo := {
  //  val nexus = "https://oss.sonatype.org/"
  //  if (version.value.endsWith("SNAPSHOT")) {
  //    Some("snapshots" at nexus + "content/repositories/snapshots")
  //  }
  //  else {
  //    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  //  }
  // }

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
)

lazy val `gdc-core` = (project in file("gdc-core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(),

    name := "gdc-core",
    spName := s"${organization.value}/${name.value}"
  ).dependsOn()

lazy val `gdc-spark_1_x` = (project in file("gdc-spark_1_x")).
  settings(commonSettings: _*).
  settings(
    sparkVersion := "1.5.0",
    testSparkVersion_1_x := sys.props.get("spark.testVersion_1_x").getOrElse(sparkVersion.value),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % testSparkVersion_1_x.value % "test" force(),
      "org.apache.spark" %% "spark-sql" % testSparkVersion_1_x.value % "test" force()),

    name := "gdc-spark_1_x",
    spName := s"${organization.value}/${name.value}",

    sparkComponents := Seq("core", "streaming")
  ).dependsOn(`gdc-core`)

lazy val `gdc-spark_2_x` = (project in file("gdc-spark_2_x")).
  settings(commonSettings: _*).
  settings(
    sparkVersion := "2.0.0",
    testSparkVersion_2_x := sys.props.get("spark.testVersion_2_x").getOrElse(sparkVersion.value),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % testSparkVersion_2_x.value % "test" force(),
      "org.apache.spark" %% "spark-sql" % testSparkVersion_2_x.value % "test" force()),

    name := "gdc-spark_2_x",
    spName := s"${organization.value}/${name.value}",

    sparkComponents := Seq("core", "streaming")
  ).dependsOn(`gdc-core`)

lazy val `gdc-google` = (project in file("gdc-google")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "com.google.api-client" % "google-api-client-java6" % "1.22.0",
      "com.google.apis" % "google-api-services-storage" % "v1-rev86-1.22.0",
      "com.google.http-client" % "google-http-client-jackson2" % "1.22.0",
      "com.google.oauth-client" % "google-oauth-client-jetty" % "1.22.0"),

    name := "gdc-google",
    spName := s"${organization.value}/${name.value}"
  ).dependsOn(`gdc-core`)

lazy val root = (project in file(".")).
  aggregate(`gdc-core`, `gdc-spark_1_x`, `gdc-spark_2_x`, `gdc-google`).
  settings(
    aggregate in update := false
  )
