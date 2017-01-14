/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional logInformation regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.spark.streaming.gdc

import java.util.Date

import es.alvsanand.gdc.core.downloader.{GdcDateSlot, GdcDownloaderFactory, GdcDownloaderParameters, GdcSlot}
import es.alvsanand.gdc.core.util.{Logging, Retry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * GdcContext creates an RDD which provides core functionality for reading
  * es.alvsanand.gdc.core.downloader.GdcSlot retrieved by
  * es.alvsanand.gdc.core.downloader.GdcDownloader.
  *
  * This is how it work:
  *
  *  - Every es.alvsanand.gdc.core.downloader.GdcSlot will be assigned to a
  * org.apache.spark.Partition in order to be distributable. However, the number of partitions
  * cannot be increased because it is impossible to now for sure if the data received by  the
  * es.alvsanand.gdc.core.downloader.GdcDownloader may be split.
  *
  *  - When a parition is compute, a new instance of
  * es.alvsanand.gdc.core.downloader.GdcDownloader is created and it is call its download()
  * method in order to retrieve the data of the name.
  *
  *  - Finally, the InputStream is parsed in order to uunzip the data if it is a GZIP name.
  *
  * @param sc The SparkContext
  */
case class GdcContext(@transient sc: SparkContext) extends Logging {

  /**
    * Create a org.apache.spark.streaming.gdc.GdcRDD from a SparkContext using a
    * es.alvsanand.gdc.core.downloader.GdcDownloaderFactory and some parameters.
    *
    * Internally, GdcContext works like this:
    *
    *  - Use the es.alvsanand.gdc.core.downloader.GdcDownloaderFactory to create a instance of
    * the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - List the slots using the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.gdc.GdcRDD.
    *
    * Note: the GdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets successful.
    *
    * @param gdcDownloaderFactory The GdcDownloaderFactory used to create the
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader.
    * @param parameters           The parameters of the
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.gdc.core.downloader.GdcSlot
    * @tparam B The type of es.alvsanand.gdc.core.downloader.GdcDownloaderParameters
    * @return A org.apache.spark.streaming.gdc.GdcRDD with all available slots.
    */
  def createGdcRDD[A <: GdcSlot : ClassTag, B <: GdcDownloaderParameters : ClassTag]
  (gdcDownloaderFactory: GdcDownloaderFactory[A, B],
   parameters: B,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    createFilteredGdcRDD[A, B](gdcDownloaderFactory, parameters, (slot: A) => true, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.gdc.GdcRDD from a SparkContext using a
    * es.alvsanand.gdc.core.downloader.GdcDownloaderFactory and some parameters.
    *
    * Internally, GdcContext works like this:
    *
    *  - Use the es.alvsanand.gdc.core.downloader.GdcDownloaderFactory to create a instance of
    * the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - List the slots using the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.gdc.GdcRDD.
    *
    * Note: the GdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param gdcDownloaderFactory The GdcDownloaderFactory used to create the
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader.
    * @param parameters           The parameters of the es.alvsanand.gdc.core.downloader
    *                             .GdcDownloader.
    * @param previousSlots        The previous slots that must be filtered.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.gdc.core.downloader.GdcSlot
    * @tparam B The type of es.alvsanand.gdc.core.downloader.GdcDownloaderParameters
    * @return A org.apache.spark.streaming.gdc.GdcRDD with all available slots.
    */
  def createPreviousFilteredGdcRDD[A <: GdcSlot : ClassTag, B <: GdcDownloaderParameters : ClassTag]
  (gdcDownloaderFactory: GdcDownloaderFactory[A, B],
   parameters: B,
   previousSlots: Seq[A],
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val filterSlot = (slot: A) => {
      !previousSlots.contains(slot)
    }

    createFilteredGdcRDD[A, B](gdcDownloaderFactory, parameters, filterSlot, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.gdc.GdcRDD from a SparkContext using a
    * es.alvsanand.gdc.core.downloader.GdcDownloaderFactory and some parameters.
    *
    * Internally, GdcContext works like this:
    *
    *  - Use the es.alvsanand.gdc.core.downloader.GdcDownloaderFactory to create a instance of
    * the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - List the slots using the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.gdc.GdcRDD.
    *
    * Note: the GdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param gdcDownloaderFactory The GdcDownloaderFactory used to create the
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader.
    * @param parameters           The parameters of the es.alvsanand.gdc.core.downloader
    *                             .GdcDownloader.
    * @param fromDate             The beginning of the date range [inclusive]
    * @param toDate               The end of the date range [exclusive]
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.gdc.core.downloader.GdcSlot
    * @tparam B The type of es.alvsanand.gdc.core.downloader.GdcDownloaderParameters
    * @return A org.apache.spark.streaming.gdc.GdcRDD with all available slots.
    */
  def createDateFilteredGdcRDD[A <: GdcDateSlot : ClassTag, B <: GdcDownloaderParameters : ClassTag]
  (gdcDownloaderFactory: GdcDownloaderFactory[A, B],
   parameters: B,
   fromDate: Option[Date] = None,
   toDate: Option[Date] = None,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val filterSlot = (gdcDateSlot: A) => {
      if ((fromDate.isDefined || toDate.isDefined) && gdcDateSlot.date == null) {
        logDebug(s"$gdcDateSlot is rejected because it needs date")
        false
      } else {
        // Inclusive
        if (fromDate.isDefined && fromDate.get.getTime > gdcDateSlot.date.getTime) {
          logDebug(s"$gdcDateSlot is rejected because date[${gdcDateSlot.date}]" +
            s" is less than $fromDate")
          false
        }
        else {
          // Exclusive
          if (toDate.isDefined && toDate.get.getTime <= gdcDateSlot.date.getTime) {
            logDebug(s"$gdcDateSlot is rejected because date[${gdcDateSlot.date}]" +
              s" is equal or greater than $toDate")
            false
          }
          else {
            true
          }
        }
      }
    }

    createFilteredGdcRDD[A, B](gdcDownloaderFactory, parameters, filterSlot, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.gdc.GdcRDD from a SparkContext using a
    * es.alvsanand.gdc.core.downloader.GdcDownloaderFactory and some parameters.
    *
    * Internally, GdcContext works like this:
    *
    *  - Use the es.alvsanand.gdc.core.downloader.GdcDownloaderFactory to create a instance of
    * the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - List the slots using the es.alvsanand.gdc.core.downloader.GdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.gdc.GdcRDD.
    *
    * Note: the GdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param gdcDownloaderFactory The GdcDownloaderFactory used to create the
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader.
    * @param parameters    The parameters of the es.alvsanand.gdc.core.downloader.GdcDownloader.
    * @param filterSlot           The filter to apply.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.gdc.core.downloader.GdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.gdc.core.downloader.GdcSlot
    * @tparam B The type of es.alvsanand.gdc.core.downloader.GdcDownloaderParameters
    * @return A org.apache.spark.streaming.gdc.GdcRDD with all available slots.
    */
  def createFilteredGdcRDD[A <: GdcSlot : ClassTag, B <: GdcDownloaderParameters : ClassTag]
  (gdcDownloaderFactory: GdcDownloaderFactory[A, B],
   parameters: B,
   filterSlot: (A) => Boolean,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val downloader = gdcDownloaderFactory.get(parameters)

    Retry(maxRetries) {
      downloader.list()
    } match {
      case Success(list) => {
        val slots: Array[A] = list.filter(s => filterSlot(s))
          .sortWith { case (a, b) => a.compare(b) < 0 }
          .toArray[A]

        logInfo(s"Detected slots[${slots.mkString(",")}] to process")

        if (slots.length == 0) {
          sc.emptyRDD[String]
        }
        else {
          new GdcRDD[A, B](sc, slots, gdcDownloaderFactory, parameters, charset, maxRetries)
        }
      }
      case Failure(e) => {
        logError(s"Error getting slots to process", e)
        sc.emptyRDD[String]
      }
    }
  }
}
