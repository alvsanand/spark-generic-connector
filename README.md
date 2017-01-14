# Generic Downloader Connector for Apache Spark

> NOTE: just now it is only compatible with in Apache Spark > 1.5.X

This is a library which enable to connect for almost every Downloadable Files Source just implementing a simple Downloader.

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/alvsanand/spark-generic-downloader-connector.svg?branch=master)

## Requirements

This library requires Spark 1.5+

## Using in Apache Spark as a package [Not yet released]

You can use Gdc in the Spark Shell adding the packages dependencies:

* Scala 2.10
```
./bin/spark-shell --packages org.apache.spark.generic-downloader-connector:gdc-spark_2x_2.10:0.2.0,\
 org.apache.spark.generic-downloader-connector:gdc-google_2.10:0.2.0,org.apache.spark.generic-downloader-connector:gdc-ftp_2.10:0.2.0
```

* Scala 2.11
```
./bin/spark-shell --packages org.apache.spark.generic-downloader-connector:gdc-spark_2x_2.11:0.2.0,\
 org.apache.spark.generic-downloader-connector:gdc-google_2.11:0.2.0,org.apache.spark.generic-downloader-connector:gdc-ftp_2.11:0.2.0
```

## Linking [Not yet released] as a Scala Library
You can link against this library in your program at the following coordinates:

### GDC Core
  * Scala 2.10
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-main_2.10
    version: 0.1.0-SNAPSHOT
    ```
  * Scala 2.11
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-main_2.11
    version: 0.1.0-SNAPSHOT
    ```

### GDC SPARK 1.X
  * Scala 2.10
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-spark_1x_2.10
    version: 0.1.0-SNAPSHOT
    ```
  * Scala 2.11
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-spark_1x_2.11
    version: 0.1.0-SNAPSHOT
    ```

### GDC SPARK 2.X
  * Scala 2.10
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-spark_2x_2.10
    version: 0.1.0-SNAPSHOT
    ```
  * Scala 2.11
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-spark_2x_2.11
    version: 0.1.0-SNAPSHOT
    ```

### GDC Google [Google Cloud Storage / Doubleclick Datatranfers]
  * Scala 2.10
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-google_2.10
    version: 0.1.0-SNAPSHOT
    ```
  * Scala 2.11
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-google_2.11
    version: 0.1.0-SNAPSHOT
    ```

### GDC VFS [FTP / SFTP / FTPS]
  * Scala 2.10
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-vfs_2.10
    version: 0.1.0-SNAPSHOT
    ```
  * Scala 2.11
    ```
    groupId: org.apache.spark.generic-downloader-connector
    artifactId: gdc-vfs_2.11
    version: 0.1.0-SNAPSHOT
    ```

## Examples
 
 There are multiple [notebooks](examples/) that show the features of the Library and how to use it correctly. 

## Building From Source
 
 To build the JAR slots simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
