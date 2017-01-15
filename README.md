# Generic Downloader Connector for Apache Spark

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/alvsanand/spark-generic-downloader-connector.svg?branch=master)

This is a package that connects almost every type of system to [Apache Spark](http://spark.apache.org/). This library can be used in batch or streaming scenarios which is awesome. From the first time, the idea is to be a _read only_ connector library. So any write operations will not be implemented.

This is are the ideas behind the library:

* The Spark core of the library is shared by all the connectors.
* Batch and streaming scenarios are compatible with the library.
* It is very easy and fast to integrate with a new system. Just extends _GdcDownloader_.
* _GdcDownloader_ has two operation:
 * `def list(): Seq[GdcSlot]`: list all the slots available to download.
 * `def download(slot: GdcSlot, out: OutputStream)`: download a slot to memory.
* A slot is the smallest part than a _GdcDownloader_ can download  in a single time. That means that the data of a slot cannot be split.
* In order to be streaming compatible the slot returned by the _GdcDownloader_ must contains date information \(_GdcDateSlot_\) in order to be sorted by time. 


> NOTE: Generic Downloader Connector is with Apache Spark > 1.5.X and only for the Scala/Java interpreter.

## Requirements

This library requires _Spark 1.5+_

## Using in Apache Spark as a package [Not yet released]

You can use _Gdc_ in the _Spark Shell_ adding the packages dependencies:

* Spark 1.x:

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

* Spark 2.x:

    * Scala 2.10
    ```
    ./bin/spark-shell --packages org.apache.spark.generic-downloader-connector:gdc-spark_1x_2.10:0.2.0,\
     org.apache.spark.generic-downloader-connector:gdc-google_2.10:0.2.0,org.apache.spark.generic-downloader-connector:gdc-ftp_2.10:0.2.0
    ```
    
    * Scala 2.11
    ```
    ./bin/spark-shell --packages org.apache.spark.generic-downloader-connector:gdc-spark_1x_2.11:0.2.0,\
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

## How to use it

Currently _Gdc_ supports two scenarios:

* Batch
* Streaming

### Batch scenario
 
In order to use the library in spark in streaming, you must follow the next steps:
 
* Import dependencies:
```
import org.apache.spark.streaming.gdc._
import es.alvsanand.gdc.ftp.{FTPCredentials, FTPSlot}
import es.alvsanand.gdc.ftp.normal.{FTPGdcDownloaderFactory, FTPParameters}
```
 
* Create a parameters object:
```
val parameters = FTPParameters("HOST", PORT, "DIRECTORY", FTPCredentials("USER", Option("PASSWORD"))
```
 
* Create the RDD passing the GdcDownloaderFactory and the parameters:
```
val rdd = sc.createDownloadRDD(FTPGdcDownloaderFactory, parameters)
```
 
* Use the RDD as desired:
```
rdd.partitions.map(_.asInstanceOf[GdcRDDPartition[GdcSlot]].slot)
rdd.saveAsTextFile("hdfs://...")
```

### Streaming scenario
 
In order to use the library in spark in batch, you must follow the next steps:
 
* Import dependencies:
```
import org.apache.spark.streaming.gdc._
import es.alvsanand.gdc.ftp.{FTPCredentials, FTPSlot}
import es.alvsanand.gdc.ftp.normal.{FTPGdcDownloaderFactory, FTPParameters}
```
 
* Create a parameters object:
```
val parameters = FTPParameters("HOST", PORT, "DIRECTORY", FTPCredentials("USER", Option("PASSWORD"))
```
 
* Create the InputDStream passing the GdcDownloaderFactory and the parameters:
```
val ssc = new StreamingContext(sc, batchTime)

val ds = ssc.createGdcInputDStream(FTPGdcDownloaderFactory, parameters, range)

ds.checkpoint(checkpointTime)

ssc.checkpoint(checkPointDirectory)
```
 
* Use the InputDStream as desired:
```
ds.foreachRDD { rdd =>
    rddrdd.saveAsTextFile("hdfs://...")
}
```

## GdcDownloader ready to use

Nowadays, Gdc has implemented the following downloaders/connector:

* Google services compatible:
    * CloudStorageGdcDownloader: is able to download files from [Google Cloud Storage](https://cloud.google.com/storage).
    * DataTransferGdcDownloader: is able to download files from [DoubleClick Data Transfer](https://support.google.com/dcm/partner/answer/165589?hl=en).

* FTP servers like:
    * FTPGdcDownloader: is able to download files from a FTP server.
    * FTPSGdcDownloader: is able to download files from a FTPS server
    * SFTPGdcDownloader: is able to download files from a SFTP server
    
> Note: for more details of every downloaders visit [Examples section](#examples)

## How to create new Downloaders

* Import dependencies:
```
import java.io._
import com.wix.accord.Validator
import com.wix.accord.dsl.{be, _}
import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException, GdcDownloaderParameters}

// Every other required dependency
```

* Create a new type of _GdcDownloaderParameters_:
```
case class RssParameters(url: String) extends GdcDownloaderParameters
```

* Create a new type of _GdcSlot_:
```
case class RssSlot(title: String, description: String, link: String, date: Date) extends GdcDateSlot
```

> Note: in case to be streaming compatible the slot must extend _GdcDateSlot_. If not, _GdcSlot_.

* Create a new type of _GdcDownloader_:
```
class RssGdcDownloader(parameters: RssParameters)
  extends GdcDownloader[RssSlot, RssParameters](parameters) {
  
  ......

}
```

* Implement the _GdcDownloader_:
    1. Override _getValidator_ in order to validates the parameters:
    ```
    override def getValidator(): Validator[RssParameters] = {
        validator[RssParameters] { p =>
          p.url is notNull
          p.url is notEmpty
        }
      }
    ```

    2. Create a client and be sure it is _Thread Safe_ or there is one instance for every Thread:
    ```
    private lazy val client: RSSClient = initClient()
    
    private def initClient(): RSSClient = synchronized {
        var client: RSSClient = null
    
        // Initialize client
      }
    ```

    3. It is also recommendable to create helper methods to use the client:
    ```
      private def connect(): Unit = {
        if (!client.isConnected) {
          Try(client.connect()) match {
            case Failure(e) => throw GdcDownloaderException(s"Error connecting to server", e)
            case _ =>
          }
        }
      }
    
      private def disconnect(): Unit = {
        if (client.isConnected) {
          client.disconnect()
        }
      }
    
      private def useClient[T](func: () => T): T = {
        Try(connect()) match {
          case Failure(e) => throw e
          case _ =>
        }
    
        val value = Try(func())
    
        Try(disconnect()) // Ignore exception in disconnecting
    
        value match {
          case Success(s) => s
          case Failure(e) => throw e
        }
      }
    ```
    
    4. Override _list_ in order to list the slots available: 
    ```
    @throws(classOf[GdcDownloaderException])
    def list(): Seq[RssSlot] = {
        var entries: Array[FeedMessage] = Array.empty
    
        Try({
          files = useClient[Array[RssSlot]](() => {
            client.listFiles(".").map(x => RssSlot(x.title, x.description, x.link, x.date: Date))
                  .sortBy(_.name).toSeq
          })
        })
        match {
          case Success(v) => v
          case Failure(e) => {
            throw GdcDownloaderException(s"Error listing messages", e)
          }
        }
      }
    ```
    
    5. Override _list_ in order to validates the parameters. 
    ```
    @throws(classOf[GdcDownloaderException])
      override def download(slot: FTPSlot, out: OutputStream): Unit = {
        Try({
          val in = useClient[InputStream](() => {
            client.retrieveFeedMessage(slot.link)
          })
    
          if (in != null) {
            IOUtils.copy(in, out)
    
            in.close()
          }
        })
        match {
          case Success(v) =>
          case Failure(e) => {
            val msg = 
            throw GdcDownloaderException(s"Error downloading slot[$slot]", e)
          }
        }
      }
    ```

* Create a new type of _GdcDownloaderFactory_:
```
object RssGdcDownloaderFactory extends GdcDownloaderFactory[RssSlot, RssParameters] {

  override def get(parameters: RssParameters): GdcDownloader[RssSlot, RssParameters] = {
    new RssGdcDownloader(parameters)
  }
}
```
    
## Examples
 
 There are multiple [notebooks](examples/) that show the features of the Library and how to use it correctly. 

## Building From Source
 
 To build the JAR slots simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
