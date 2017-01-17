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

package org.apache.spark.streaming.sgc

import java.util.Date

import es.alvsanand.sgc.core.connector.{SgcDateSlot, SgcConnectorFactory, SgcConnectorParameters, SgcSlot}
import es.alvsanand.sgc.core.util.{Logging, Retry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * SgcContext creates an RDD which provides core functionality for reading
  * es.alvsanand.sgc.core.connector.SgcSlot retrieved by
  * es.alvsanand.sgc.core.connector.SgcConnector.
  *
  * This is how it work:
  *
  *  - Every es.alvsanand.sgc.core.connector.SgcSlot will be assigned to a
  * org.apache.spark.Partition in order to be distributable. However, the number of partitions
  * cannot be increased because it is impossible to now for sure if the data received by  the
  * es.alvsanand.sgc.core.connector.SgcConnector may be split.
  *
  *  - When a parition is compute, a new instance of
  * es.alvsanand.sgc.core.connector.SgcConnector is created and it is call its fetch()
  * method in order to retrieve the data of the name.
  *
  *  - Finally, the InputStream is parsed in order to uunzip the data if it is a GZIP name.
  *
  * @param sc The SparkContext
  */
case class SgcContext(@transient sc: SparkContext) extends Logging {

  /**
    * Create a org.apache.spark.streaming.sgc.SgcRDD from a SparkContext using a
    * es.alvsanand.sgc.core.connector.SgcConnectorFactory and some parameters.
    *
    * Internally, SgcContext works like this:
    *
    *  - Use the es.alvsanand.sgc.core.connector.SgcConnectorFactory to create a instance of
    * the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - List the slots using the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgc.SgcRDD.
    *
    * Note: the SgcContext is fault tolerant and is able to retry n times until the list() operation
    * gets successful.
    *
    * @param sgcConnectorFactory The SgcConnectorFactory used to create the
    *                             es.alvsanand.sgc.core.connector.SgcConnector.
    * @param parameters           The parameters of the
    *                             es.alvsanand.sgc.core.connector.SgcConnector.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             fetched.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgc.core.connector.SgcConnector is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
    * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
    * @return A org.apache.spark.streaming.sgc.SgcRDD with all available slots.
    */
  def createSgcRDD[A <: SgcSlot : ClassTag, B <: SgcConnectorParameters : ClassTag]
  (sgcConnectorFactory: SgcConnectorFactory[A, B],
   parameters: B,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    createFilteredSgcRDD[A, B](sgcConnectorFactory, parameters, (slot: A) => true, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.sgc.SgcRDD from a SparkContext using a
    * es.alvsanand.sgc.core.connector.SgcConnectorFactory and some parameters.
    *
    * Internally, SgcContext works like this:
    *
    *  - Use the es.alvsanand.sgc.core.connector.SgcConnectorFactory to create a instance of
    * the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - List the slots using the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgc.SgcRDD.
    *
    * Note: the SgcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param sgcConnectorFactory The SgcConnectorFactory used to create the
    *                             es.alvsanand.sgc.core.connector.SgcConnector.
    * @param parameters           The parameters of the es.alvsanand.sgc.core.connector
    *                             .SgcConnector.
    * @param previousSlots        The previous slots that must be filtered.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             fetched.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgc.core.connector.SgcConnector is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
    * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
    * @return A org.apache.spark.streaming.sgc.SgcRDD with all available slots.
    */
  def createPreviousFilteredSgcRDD[A <: SgcSlot : ClassTag, B <: SgcConnectorParameters : ClassTag]
  (sgcConnectorFactory: SgcConnectorFactory[A, B],
   parameters: B,
   previousSlots: Seq[A],
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val filterSlot = (slot: A) => {
      !previousSlots.contains(slot)
    }

    createFilteredSgcRDD[A, B](sgcConnectorFactory, parameters, filterSlot, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.sgc.SgcRDD from a SparkContext using a
    * es.alvsanand.sgc.core.connector.SgcConnectorFactory and some parameters.
    *
    * Internally, SgcContext works like this:
    *
    *  - Use the es.alvsanand.sgc.core.connector.SgcConnectorFactory to create a instance of
    * the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - List the slots using the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgc.SgcRDD.
    *
    * Note: the SgcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param sgcConnectorFactory The SgcConnectorFactory used to create the
    *                             es.alvsanand.sgc.core.connector.SgcConnector.
    * @param parameters           The parameters of the es.alvsanand.sgc.core.connector
    *                             .SgcConnector.
    * @param fromDate             The beginning of the date range [inclusive]
    * @param toDate               The end of the date range [exclusive]
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             fetched.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgc.core.connector.SgcConnector is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
    * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
    * @return A org.apache.spark.streaming.sgc.SgcRDD with all available slots.
    */
  def createDateFilteredSgcRDD[A <: SgcDateSlot : ClassTag, B <: SgcConnectorParameters : ClassTag]
  (sgcConnectorFactory: SgcConnectorFactory[A, B],
   parameters: B,
   fromDate: Option[Date] = None,
   toDate: Option[Date] = None,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val filterSlot = (sgcDateSlot: A) => {
      if ((fromDate.isDefined || toDate.isDefined) && sgcDateSlot.date == null) {
        logDebug(s"$sgcDateSlot is rejected because it needs date")
        false
      } else {
        // Inclusive
        if (fromDate.isDefined && fromDate.get.getTime > sgcDateSlot.date.getTime) {
          logDebug(s"$sgcDateSlot is rejected because date[${sgcDateSlot.date}]" +
            s" is less than $fromDate")
          false
        }
        else {
          // Exclusive
          if (toDate.isDefined && toDate.get.getTime <= sgcDateSlot.date.getTime) {
            logDebug(s"$sgcDateSlot is rejected because date[${sgcDateSlot.date}]" +
              s" is equal or greater than $toDate")
            false
          }
          else {
            true
          }
        }
      }
    }

    createFilteredSgcRDD[A, B](sgcConnectorFactory, parameters, filterSlot, charset, maxRetries)
  }

  /**
    * Create a org.apache.spark.streaming.sgc.SgcRDD from a SparkContext using a
    * es.alvsanand.sgc.core.connector.SgcConnectorFactory and some parameters.
    *
    * Internally, SgcContext works like this:
    *
    *  - Use the es.alvsanand.sgc.core.connector.SgcConnectorFactory to create a instance of
    * the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - List the slots using the es.alvsanand.sgc.core.connector.SgcConnector.
    *
    *  - Optionally, filter the name list if a range of dates has been selected.
    *
    *  - Create a org.apache.spark.streaming.sgc.SgcRDD.
    *
    * Note: the SgcContext is fault tolerant and is able to retry n times until the list() operation
    * gets succesful.
    *
    * @param sgcConnectorFactory The SgcConnectorFactory used to create the
    *                             es.alvsanand.sgc.core.connector.SgcConnector.
    * @param parameters    The parameters of the es.alvsanand.sgc.core.connector.SgcConnector.
    * @param filterSlot           The filter to apply.
    * @param charset              The java.nio.charset.Charset name of the slots that are
    *                             going to be
    *                             fetched.
    * @param maxRetries           The maximum number times that an operation of a
    *                             es.alvsanand.sgc.core.connector.SgcConnector is going to
    *                             be repeated
    *                             in case of failure.
    * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
    * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
    * @return A org.apache.spark.streaming.sgc.SgcRDD with all available slots.
    */
  def createFilteredSgcRDD[A <: SgcSlot : ClassTag, B <: SgcConnectorParameters : ClassTag]
  (sgcConnectorFactory: SgcConnectorFactory[A, B],
   parameters: B,
   filterSlot: (A) => Boolean,
   charset: String = "UTF-8",
   maxRetries: Int = 3
  ): RDD[String] = {
    val connector = sgcConnectorFactory.get(parameters)

    Retry(maxRetries) {
      connector.list()
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
          new SgcRDD[A, B](sc, slots, sgcConnectorFactory, parameters, charset, maxRetries)
        }
      }
      case Failure(e) => {
        logError(s"Error getting slots to process", e)
        sc.emptyRDD[String]
      }
    }
  }
}
