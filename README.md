# Generic Downloader Connector for Apache Spark

> NOTE: just now it is only compatible with in Apache Spark > 1.5.X and < 2.1.X

This is a library which enable to connect for almost every Downloadable Files Source just implementing a simple Downloader.

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/alvsanand/spark-generic-downloader-connector.svg?branch=master)

## Requirements

This library requires Spark 1.5+

## Linking [Not yet released]
You can link against this library in your program at the following coordinates:

### GDC Core

#### Scala 2.10
```
groupId: org.apache.spark.generic-downloader-connector
artifactId: gdc-main_2.10
version: 0.1.0-SNAPSHOT
```
#### Scala 2.11
```
groupId: org.apache.spark.generic-downloader-connector
artifactId: gdc-main_2.11
version: 0.1.0-SNAPSHOT
```

### GDC Google

#### Scala 2.10
```
groupId: org.apache.spark.generic-downloader-connector
artifactId: gdc-google_2.10
version: 0.1.0-SNAPSHOT
```
#### Scala 2.11
```
groupId: org.apache.spark.generic-downloader-connector
artifactId: gdc-google_2.11
version: 0.1.0-SNAPSHOT
```

## Building From Source
 
 To build the JAR files simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
