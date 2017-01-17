# Generic Connector for Apache Spark

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/alvsanand/spark-generic-connector-connector.svg?branch=master)

This library simplifies the connection of a external system with [Apache Spark](http://spark.apache.org/). Its main idea is to use a core functionality that is responsible of working with Apache Spark and implement specific connectors for any system. It can be used in batch or streaming scenarios which is awesome. From the first time, the idea is to be a _read only_ connector library. So any write operations will not be implemented.

This is are the ideas behind the library:

* The Spark core of the library is shared by all the connectors.
* Batch and streaming scenarios are compatible with the library.
* It is very easy and fast to integrate with a new system. Just extends _SgcConnector_.
* _SgcConnector_ has two operation:
 * `def list(): Seq[SgcSlot]`: list all the slots available to fetch.
 * `def fetch(slot: SgcSlot, out: OutputStream)`: fetch a slot to memory.
* A slot is the smallest part than a _SgcConnector_ can fetch  in a single time. That means that the data of a slot cannot be split.
* In order to be streaming compatible the slot returned by the _SgcConnector_ must contains date information \(_SgcDateSlot_\) in order to be sorted by time. 


> NOTE: Generic Connector is with Apache Spark > 1.5.X and only for the Scala/Java interpreter.

## Requirements

This library requires _Spark 1.5+_

## Using in Apache Spark as a package [Not yet released]

You can use _Generic Connector_ in the _Spark Shell_ adding the packages dependencies:

* Spark 1.x:

    * Scala 2.10
    
           ./bin/spark-shell --packages es.alvsanand:spark-generic-connector-spark_1x_2.10:0.2.0
    
    * Scala 2.11
    
           ./bin/spark-shell --packages es.alvsanand:spark-generic-connector-spark_1x_2.11:0.2.0

* Spark 2.x:

    * Scala 2.10
    
           ./bin/spark-shell --packages es.alvsanand:spark-generic-connector-spark_2x_2.10:0.2.0
    
    * Scala 2.11
    
           ./bin/spark-shell --packages es.alvsanand:spark-generic-connector-spark_2x_2.11:0.2.0

## Linking [Not yet released] as a Scala Library
You can link against this library in your program at the following coordinates:

### Generic Connector core

  * Scala 2.10
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-main_2.10
         version: 0.2.0
    
  * Scala 2.11
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-main_2.11
         version: 0.2.0

### Generic Connector for Apache Spark 1.X

  * Scala 2.10
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-spark_1x_2.10
         version: 0.2.0
    
  * Scala 2.11
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-spark_1x_2.11
         version: 0.2.0

### Generic Connector for Apache Spark 2.X

  * Scala 2.10
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-spark_2x_2.10
         version: 0.2.0
    
  * Scala 2.11
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-spark_2x_2.11
         version: 0.2.0

### Google connectors [Google Cloud Storage / Doubleclick Datatranfers]

  * Scala 2.10
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-google_2.10
         version: 0.2.0
    
  * Scala 2.11
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-google_2.11
         version: 0.2.0

### FTP connectors [FTP / SFTP / FTPS]

  * Scala 2.10
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-ftp_2.10
         version: 0.2.0
    
  * Scala 2.11
  
         groupId: es.alvsanand
         artifactId: spark-generic-connector-ftp_2.11
         version: 0.2.0

## How to use it

Currently _Sgc_ supports two scenarios:

* Batch
* Streaming

### Batch scenario
 
In order to use the library with _Apache Spark_ in batch mode, you must follow the next steps:
 
* Import dependencies:

       import org.apache.spark.streaming.sgc._
       import es.alvsanand.sgc.ftp.{FTPCredentials, FTPSlot}
       import es.alvsanand.sgc.ftp.normal.{FTPSgcConnectorFactory, FTPParameters}
 
* Create a parameters object:

       val parameters = FTPParameters("HOST", PORT, "DIRECTORY", FTPCredentials("USER", Option("PASSWORD"))
 
* Create the RDD passing the SgcConnectorFactory and the parameters:

       val rdd = sc.createSgcRDD(FTPSgcConnectorFactory, parameters)
 
* Use the RDD as desired:

       rdd.partitions.map(_.asInstanceOf[SgcRDDPartition[SgcSlot]].slot)
       rdd.saveAsTextFile("hdfs://...")

### Streaming scenario
 
In order to use the library with _Apache Spark_ in streaming mode, you must follow the next steps:
 
* Import dependencies:

       import org.apache.spark.streaming.sgc._
       import es.alvsanand.sgc.ftp.{FTPCredentials, FTPSlot}
       import es.alvsanand.sgc.ftp.normal.{FTPSgcConnectorFactory, FTPParameters}
 
* Create a parameters object:

       val parameters = FTPParameters("HOST", PORT, "DIRECTORY", FTPCredentials("USER", Option("PASSWORD"))
 
* Create the InputDStream passing the SgcConnectorFactory and the parameters:

       val ssc = new StreamingContext(sc, batchTime)
       
       val ds = ssc.createSgcInputDStream(FTPSgcConnectorFactory, parameters, range)
       
       ds.checkpoint(checkpointTime)
       
       ssc.checkpoint(checkPointDirectory)
 
* Use the InputDStream as desired:

       ds.foreachRDD { rdd =>
           rddrdd.saveAsTextFile("hdfs://...")
       }

## SgcConnector ready to use

Nowadays, Sgc has implemented the following connectors:

* Google services:
    * CloudStorageSgcConnector: is able to fetch files from [Google Cloud Storage](https://cloud.google.com/storage).
    * DataTransferSgcConnector: is able to fetch files from [DoubleClick Data Transfer](https://support.google.com/dcm/partner/answer/165589?hl=en).

* FTP servers like:
    * FTPSgcConnector: is able to fetch files from a FTP server.
    * FTPSSgcConnector: is able to fetch files from a FTPS server
    * SFTPSgcConnector: is able to fetch files from a SFTP server
    
> Note: for more details of every connectors visit [Examples section](#examples)

## How to create new Connectors

* Import dependencies:

       import java.io._
       import com.wix.accord.Validator
       import com.wix.accord.dsl.{be, _}
       import es.alvsanand.sgc.core.connector.{SgcConnector, SgcConnectorException, SgcConnectorParameters}
       
       // Every other required dependency

* Create a new type of _SgcConnectorParameters_:

       case class RssParameters(url: String) extends SgcConnectorParameters

* Create a new type of _SgcSlot_:

    case class RssSlot(title: String, description: String, link: String, date: Date) extends SgcDateSlot

> Note: in case to be streaming compatible the slot must extend _SgcDateSlot_. If not, _SgcSlot_.

* Create a new type of _SgcConnector_:

      class RssSgcConnector(parameters: RssParameters)
        extends SgcConnector[RssSlot, RssParameters](parameters) {
        
        ......
      
      }

* Implement the _SgcConnector_:

    1. Override _getValidator_ in order to validates the parameters:
    
           override def getValidator(): Validator[RssParameters] = {
               validator[RssParameters] { p =>
                 p.url is notNull
                 p.url is notEmpty
               }
            }

    2. Create a client and be sure it is _Thread Safe_ or there is one instance for every Thread:
    
           private lazy val client: RSSClient = initClient()
           
           private def initClient(): RSSClient = synchronized {
               var client: RSSClient = null
           
               // Initialize client
           }

    3. It is also recommendable to create helper methods to use the client:
    
           private def connect(): Unit = {
             if (!client.isConnected) {
               Try(client.connect()) match {
                 case Failure(e) => throw SgcConnectorException(s"Error connecting to server", e)
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
    
    4. Override _list_ in order to list the slots available: 
    
           @throws(classOf[SgcConnectorException])
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
                   throw SgcConnectorException(s"Error listing messages", e)
                 }
               }
           }
    
    5. Override _list_ in order to validates the parameters:
    
           @throws(classOf[SgcConnectorException])
             override def fetch(slot: FTPSlot, out: OutputStream): Unit = {
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
                   throw SgcConnectorException(s"Error fetching slot[$slot]", e)
                 }
               }
           }

* Create a new type of _SgcConnectorFactory_:

       object RssSgcConnectorFactory extends SgcConnectorFactory[RssSlot, RssParameters] {
       
         override def get(parameters: RssParameters): SgcConnector[RssSlot, RssParameters] = {
           new RssSgcConnector(parameters)
         }
       }
    
## Scaladoc
 
 [Here](docs/scaladoc/index.html) you can see the Scala API documentation of the project. 
    
## Examples
 
 There are multiple [notebooks](https://github.com/alvsanand/spark-generic-connector/tree/master/examples) that show the features of the Library and how to use it correctly. 

## Building From Source
 
 To build the JAR slots simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
