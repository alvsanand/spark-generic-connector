import sbt.ExclusionRule
import UnidocKeys._

val `testSparkVersion_1x` = settingKey[String]("The version of Spark to test against.")
val `testSparkVersion_2x` = settingKey[String]("The version of Spark to test against.")

val `defaultSparkVersion_1x` = settingKey[String]("The default version of Spark 1.x")
val `defaultSparkVersion_2x` = settingKey[String]("The default version of Spark 2.x")

val sparkVersion = settingKey[String]("The version of Spark")

parallelExecution in ThisBuild := false

lazy val commonSettings = Seq(
  organization := "es.alvsanand",
  name := "spark-generic-connector-main",
  version := "0.2.1-SNAPSHOT",

  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.5", "2.11.8"),

  `defaultSparkVersion_1x` := "1.6.0",
  `defaultSparkVersion_2x` := "2.1.0",

  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",

    "org.scalatest" %% "scalatest" % "2.2.1" % "provided",
    "org.mockito" % "mockito-core" % "1.10.19" % "provided"),

  // Exlcude Sclala libraries in assembly
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter {_.data.getName.matches("scalatest.*")}
  },

//  scalacOptions in (ScalaUnidoc, unidoc) += "-Ymacro-no-expand",

  publishMavenStyle := true,

  // Skip tests during assembly
  test in assembly := {},

  // publishTo := {
  //  val nexus = "https://oss.sonatype.org/"
  //  if (version.value.endsWith("SNAPSHOT")) {
  //    Some("snapshots" at nexus + "content/repositories/snapshots")
  //  }
  //  else {
  //    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  //  }
  // },

  pomExtra := (
    <url>https://github.com/alvsanand/spark-generic-connector</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:alvsanand/spark-generic-connector.git</url>
        <connection>scm:git:git@github.com:alvsanand/spark-generic-connector
          .git</connection>
      </scm>
      <developers>
        <developer>
          <id>alvsanand</id>
          <name>Alvaro Santos Andres</name>
        </developer>
      </developers>)
)

lazy val `sgc-core` = (project in file("sgc-core")).
  settings(commonSettings: _*).
  settings(
    name := "spark-generic-connector-core",

    assembleArtifact in assembly := false,

    libraryDependencies ++= Seq(
      ("com.wix" %% "accord-core" % "0.6.1").exclude("org.scala-lang", "scala-library"),

      "org.slf4j" % "slf4j-api" % "1.7.16" % "provided",
      "org.slf4j" % "slf4j-log4j12" % "1.7.16" % "provided",
      "log4j" % "log4j" % "1.2.16" % "provided",
      "commons-io" % "commons-io" % "2.4" % "provided",

      "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0" % "provided",

      "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0" % "test" classifier "tests",
      "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "test" classifier "tests"
    )
  )

lazy val `sgc-google` = (project in file("sgc-google")).dependsOn(`sgc-core`).
  settings(commonSettings: _*).
  settings(
    name := "spark-generic-connector-google",

    assembleArtifact in assembly := false,

    libraryDependencies ++= Seq(
      "com.google.api-client" % "google-api-client-java6" % "1.22.0",
      "com.google.apis" % "google-api-services-storage" % "v1-rev86-1.22.0",
      "com.google.http-client" % "google-http-client-jackson2" % "1.22.0",
      ("com.google.oauth-client" % "google-oauth-client-jetty" % "1.22.0")
        .excludeAll(ExclusionRule("org.eclipse.jetty.orbit", "javax.servlet"),
                    ExclusionRule("org.eclipse.jetty.orbit", "servlet-api")),

      "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",

      "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0" % "provided"
    )
  )

lazy val `sgc-ftp` = (project in file("sgc-ftp")).dependsOn(`sgc-core`).
  settings(commonSettings: _*).
  settings(
    name := "spark-generic-connector-ftp",

    assembleArtifact in assembly := false,

    libraryDependencies ++= Seq(
      "com.jcraft" % "jsch" % "0.1.54",

      "org.apache.ftpserver" % "ftpserver-core" % "1.1.0" % "test",
      "org.apache.sshd" % "sshd-core" % "1.3.0" % "test",


      "commons-net" % "commons-net" % "2.2" % "provided",
      "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0" % "provided"
    )
  )

lazy val `sgc-spark_1x` = (project in file("sgc-spark_1x")).dependsOn(`sgc-core`, `sgc-google`, `sgc-ftp`).
  settings(commonSettings: _*).
  settings(
    name := "spark-generic-connector-spark_1x",

    assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}_assembly.jar",

    sparkVersion := `defaultSparkVersion_1x`.value,
    `testSparkVersion_1x` := sys.props.get("spark.testVersion_1x").getOrElse(sparkVersion.value),

    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-core" % `testSparkVersion_1x`.value % "provided")
        .excludeAll(ExclusionRule("org.eclipse.jetty.orbit", "javax.servlet"),
                    ExclusionRule("org.eclipse.jetty.orbit", "servlet-api")),
      ("org.apache.spark" %% "spark-streaming" % `testSparkVersion_1x`.value % "provided")
        .excludeAll(ExclusionRule("org.eclipse.jetty.orbit", "javax.servlet"),
                    ExclusionRule("org.eclipse.jetty.orbit", "servlet-api")),

      "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided"
    ),

    unmanagedSourceDirectories in Compile +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/main/scala",
    unmanagedSourceDirectories in Test +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/test/scala",
    unmanagedResourceDirectories in Compile +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/main/resources",
    unmanagedResourceDirectories in Test +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/test/resources"
  )

lazy val `sgc-spark_2x` = (project in file("sgc-spark_2x")).dependsOn(`sgc-core`, `sgc-google`, `sgc-ftp`).
  settings(commonSettings: _*).
  settings(
    name := "spark-generic-connector-spark_2x",

    assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}_assembly.jar",

    sparkVersion := `defaultSparkVersion_2x`.value,
    `testSparkVersion_2x` := sys.props.get("spark.testVersion_2x").getOrElse(sparkVersion.value),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % `testSparkVersion_2x`.value % "provided",
      "org.apache.spark" %% "spark-streaming" % `testSparkVersion_2x`.value % "provided"
    ),

    unmanagedSourceDirectories in Compile +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/main/scala",
    unmanagedSourceDirectories in Test +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/test/scala",
    unmanagedResourceDirectories in Compile +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/main/resources",
    unmanagedResourceDirectories in Test +=
      baseDirectory.value.getParentFile() / "sgc-spark/src/test/resources"
  )

lazy val root = (project in file(".")).
  aggregate(`sgc-core`, `sgc-google`, `sgc-ftp`, `sgc-spark_1x`, `sgc-spark_2x`).
  settings(commonSettings: _*).
  settings(unidocSettings: _*).
  settings(
    aggregate in update := false,
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(`sgc-spark_1x`)
  )
