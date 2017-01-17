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

import java.io._
import java.util.concurrent.atomic.AtomicLong

import es.alvsanand.sgc.core.connector._
import es.alvsanand.sgc.core.util.{IOUtils, Retry}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ShutdownHookManager

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

/**
  * An RDD that provides core functionality for reading es.alvsanand.sgc.core.connector.SgcSlot
  * retrieved by es.alvsanand.sgc.core.connector.SgcConnector.
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
  * @param slots The list of slots
  * @param sgcConnectorFactory The SgcConnectorFactory used to create the
  *                             es.alvsanand.sgc.core.connector.SgcConnector.
  * @param parameters The parameters of the es.alvsanand.sgc.core.connector.SgcConnector.
  * @param charset The java.nio.charset.Charset name of the slots that are going to be
  *                fetched.
  * @param maxRetries The maximum number times that an operation of a
  *                   es.alvsanand.sgc.core.connector.SgcConnector is going to be repeated
  *                   in case of failure.
  * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
  * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
  */
private[sgc]
class SgcRDD[A <: SgcSlot: ClassTag, B <: SgcConnectorParameters: ClassTag](sc: SparkContext,
                                                                             slots: Array[A],
                                                                             sgcConnectorFactory: SgcConnectorFactory[A, B],
                                                                             parameters: B,
                                                                             charset: String = "UTF-8",
                                                                             maxRetries: Int = 3
                                     ) extends RDD[String](sc, Nil) with
  HasSgcFileRange {

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD]] */
  override def compute(split: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val iter = new Iterator[String] {
      logInfo("Input partition: " + split)

      val metrics: SgcRDDMetrics = new SgcRDDMetrics(context)

      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false

      val outputStream: Try[ByteArrayOutputStream] = Retry(maxRetries) {
        fetchSlot(split.asInstanceOf[SgcRDDPartition[A]].slot)
      }

      var reader: BufferedReader = null

      outputStream match {
        case Success(v) => {
          reader = new BufferedReader(new InputStreamReader(
            IOUtils.parseInputStream(new ByteArrayInputStream(v.toByteArray))
          ))
        }
        case Failure(e) => {
          val msg = s"Error fetching partition[$split] after $maxRetries tries"
          logError(msg)

          throw SgcConnectorException(msg, e)
        }
      }

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.ready
          if (finished) {
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): String = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false

        val line = reader.readLine()

        if (!finished && line!=null) {
          metrics.updateRecordRead()
          metrics.updateBytesRead(line.length)
        }

        line
      }

      private def close() {
        logInfo("Finished partition: " + split)

        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }

          metrics.updateBytesRead()
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  /**
    * Internal method that fetch a es.alvsanand.sgc.core.connector.SgcSlot using the
    * specific es.alvsanand.sgc.core.connector.SgcConnector. Due to the unknown nature of the
    * es.alvsanand.sgc.core.connector.SgcConnector, it cannot be used a shared instance
    * between threads.
 *
    * @param slot The slot to be fetched
    * @return The data of the slot
    */
  private def fetchSlot(slot: A): ByteArrayOutputStream = {
    logInfo(s"Fetching slot[$slot]")

    val sgcConnector = sgcConnectorFactory.get(parameters)

    val outputStream = new ByteArrayOutputStream()

    sgcConnector.fetch(slot, outputStream)

    outputStream
  }

  /**
    * Returns the current slots that are going to be processed by this RDD.
    * @return A SgcRange of slots
    */
  override def range(): SgcRange = {
    if (slots.isEmpty) {
      SgcRange.Empty
    }
    else {
      if (slots.head.isInstanceOf[SgcDateSlot]) {
        SgcRange(slots.map(_.asInstanceOf[SgcDateSlot].date).sorted
          .headOption.getOrElse(null)
          , slots.map(_.name).toSeq)
      }
      else{
        SgcRange(slots.map(_.name).toSeq)
      }
    }
  }

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD]] */
  override protected def getPartitions: Array[Partition] = {
    slots.zipWithIndex.map { case (f, i) => new SgcRDDPartition(f, i) }
  }
}

/**
  * This trait represent an object which has a org.apache.spark.streaming.sgc.SgcRange.
  */
trait HasSgcFileRange {
  /**
    * Return the current SgcRange
    * @return A SgcRange
    */
  def range: SgcRange
}
