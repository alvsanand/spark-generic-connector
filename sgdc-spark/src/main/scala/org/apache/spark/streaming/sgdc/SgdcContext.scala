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

package org.apache.spark.streaming.sgdc

import java.util.Date

import es.alvsanand.sgdc.core.downloader.{SgdcDateSlot, SgdcDownloaderFactory, SgdcDownloaderParameters, SgdcSlot}
import es.alvsanand.sgdc.core.util.{Logging, Retry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * SgdcContext creates an RDD which provides core functionality for reading
  * es.alvsanand.sgdc.core.downloader.SgdcSlot retrieved by
  * es.alvsanand.sgdc.core.downloader.SgdcDownloader.
  *
  * This is how it work:
  *
  *  - Every es.alvsanand.sgdc.core.downloader.SgdcSlot will be assigned to a
  * org.apache.spark.Partition in order to be distributable. However, the number of partitions
  * cannot be increased because it is impossible to now for sure if the data received by  the
  * es.alvsanand.sgdc.core.downloader.SgdcDownloader may be split.
  *
  *  - When a parition is compute, a new instance of
  * es.alvsanand.sgdc.core.downloader.SgdcDownloader is created and it is call its download()
  * method in order to retrieve the data of the name.
  *
  *  - Finally, the InputStream is parsed in order to uunzip the data if it is a GZIP name.
  *
  * @param sc The SparkContext
  */
case class SgdcContext(@transient sc: SparkContext) extends Logging {

  /**
    * Create a org.apache.spark.streaming.sgdc.SgdcRDD from a SparkContext using a
    * es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory and some parameters.
    *
    * Internally, SgdcContext works like this:
    *
    *  - Use the es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory to create a instance of
    * the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - List the slots using the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgdc.SgdcRDD.
    *
    * Note: the SgdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets successful.
    *
    * @param sgdcDownloaderFactory The SgdcDownloaderFactory used to create the
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param parameters           The parameters of the
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgdc.core.downloader.SgdcSlot
    * @tparam B The type of es.alvsanand.sgdc.core.downloader.SgdcDownloaderParameters
    * @return A org.apache.spark.streaming.sgdc.SgdcRDD with all available slots.
    */
  def createSgdcRDD[A <: SgdcSlot : ClassTag, B <: SgdcDownloaderParameters : ClassTag]
  (sgdcDownloaderFactory: SgdcDownloaderFactory[A, B],
   parameters: B,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    createFilteredSgdcRDD[A, B](sgdcDownloaderFactory, parameters, (slot: A) => true, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.sgdc.SgdcRDD from a SparkContext using a
    * es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory and some parameters.
    *
    * Internally, SgdcContext works like this:
    *
    *  - Use the es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory to create a instance of
    * the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - List the slots using the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgdc.SgdcRDD.
    *
    * Note: the SgdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param sgdcDownloaderFactory The SgdcDownloaderFactory used to create the
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param parameters           The parameters of the es.alvsanand.sgdc.core.downloader
    *                             .SgdcDownloader.
    * @param previousSlots        The previous slots that must be filtered.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgdc.core.downloader.SgdcSlot
    * @tparam B The type of es.alvsanand.sgdc.core.downloader.SgdcDownloaderParameters
    * @return A org.apache.spark.streaming.sgdc.SgdcRDD with all available slots.
    */
  def createPreviousFilteredSgdcRDD[A <: SgdcSlot : ClassTag, B <: SgdcDownloaderParameters : ClassTag]
  (sgdcDownloaderFactory: SgdcDownloaderFactory[A, B],
   parameters: B,
   previousSlots: Seq[A],
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val filterSlot = (slot: A) => {
      !previousSlots.contains(slot)
    }

    createFilteredSgdcRDD[A, B](sgdcDownloaderFactory, parameters, filterSlot, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.sgdc.SgdcRDD from a SparkContext using a
    * es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory and some parameters.
    *
    * Internally, SgdcContext works like this:
    *
    *  - Use the es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory to create a instance of
    * the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - List the slots using the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgdc.SgdcRDD.
    *
    * Note: the SgdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param sgdcDownloaderFactory The SgdcDownloaderFactory used to create the
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param parameters           The parameters of the es.alvsanand.sgdc.core.downloader
    *                             .SgdcDownloader.
    * @param fromDate             The beginning of the date range [inclusive]
    * @param toDate               The end of the date range [exclusive]
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgdc.core.downloader.SgdcSlot
    * @tparam B The type of es.alvsanand.sgdc.core.downloader.SgdcDownloaderParameters
    * @return A org.apache.spark.streaming.sgdc.SgdcRDD with all available slots.
    */
  def createDateFilteredSgdcRDD[A <: SgdcDateSlot : ClassTag, B <: SgdcDownloaderParameters : ClassTag]
  (sgdcDownloaderFactory: SgdcDownloaderFactory[A, B],
   parameters: B,
   fromDate: Option[Date] = None,
   toDate: Option[Date] = None,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val filterSlot = (sgdcDateSlot: A) => {
      if ((fromDate.isDefined || toDate.isDefined) && sgdcDateSlot.date == null) {
        logDebug(s"$sgdcDateSlot is rejected because it needs date")
        false
      } else {
        // Inclusive
        if (fromDate.isDefined && fromDate.get.getTime > sgdcDateSlot.date.getTime) {
          logDebug(s"$sgdcDateSlot is rejected because date[${sgdcDateSlot.date}]" +
            s" is less than $fromDate")
          false
        }
        else {
          // Exclusive
          if (toDate.isDefined && toDate.get.getTime <= sgdcDateSlot.date.getTime) {
            logDebug(s"$sgdcDateSlot is rejected because date[${sgdcDateSlot.date}]" +
              s" is equal or greater than $toDate")
            false
          }
          else {
            true
          }
        }
      }
    }

    createFilteredSgdcRDD[A, B](sgdcDownloaderFactory, parameters, filterSlot, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.sgdc.SgdcRDD from a SparkContext using a
    * es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory and some parameters.
    *
    * Internally, SgdcContext works like this:
    *
    *  - Use the es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory to create a instance of
    * the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - List the slots using the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgdc.SgdcRDD.
    *
    * Note: the SgdcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param sgdcDownloaderFactory The SgdcDownloaderFactory used to create the
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param parameters    The parameters of the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param filterSlot           The filter to apply.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             downloaded.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgdc.core.downloader.SgdcSlot
    * @tparam B The type of es.alvsanand.sgdc.core.downloader.SgdcDownloaderParameters
    * @return A org.apache.spark.streaming.sgdc.SgdcRDD with all available slots.
    */
  def createFilteredSgdcRDD[A <: SgdcSlot : ClassTag, B <: SgdcDownloaderParameters : ClassTag]
  (sgdcDownloaderFactory: SgdcDownloaderFactory[A, B],
   parameters: B,
   filterSlot: (A) => Boolean,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val downloader = sgdcDownloaderFactory.get(parameters)

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
          new SgdcRDD[A, B](sc, slots, sgdcDownloaderFactory, parameters, charset, maxRetries)
        }
      }
      case Failure(e) => {
        logError(s"Error getting slots to process", e)
        sc.emptyRDD[String]
      }
    }
  }
}
