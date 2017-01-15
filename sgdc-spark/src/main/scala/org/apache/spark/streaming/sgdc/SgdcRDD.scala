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

import java.io._
import java.util.concurrent.atomic.AtomicLong

import es.alvsanand.sgdc.core.downloader._
import es.alvsanand.sgdc.core.util.{IOUtils, Retry}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ShutdownHookManager

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

/**
  * An RDD that provides core functionality for reading es.alvsanand.sgdc.core.downloader.SgdcSlot
  * retrieved by es.alvsanand.sgdc.core.downloader.SgdcDownloader.
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
  * @param slots The list of slots
  * @param sgdcDownloaderFactory The SgdcDownloaderFactory used to create the
  *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
  * @param parameters The parameters of the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
  * @param charset The java.nio.charset.Charset name of the slots that are going to be
  *                downloaded.
  * @param maxRetries The maximum number times that an operation of a
  *                   es.alvsanand.sgdc.core.downloader.SgdcDownloader is going to be repeated
  *                   in case of failure.
  * @tparam A The type of es.alvsanand.sgdc.core.downloader.SgdcSlot
  * @tparam B The type of es.alvsanand.sgdc.core.downloader.SgdcDownloaderParameters
  */
private[sgdc]
class SgdcRDD[A <: SgdcSlot: ClassTag, B <: SgdcDownloaderParameters: ClassTag](sc: SparkContext,
                                                                             slots: Array[A],
                                                                             sgdcDownloaderFactory: SgdcDownloaderFactory[A, B],
                                                                             parameters: B,
                                                                             charset: String = "UTF-8",
                                                                             maxRetries: Int = 3
                                     ) extends RDD[String](sc, Nil) with
  HasSgdcFileRange {

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD]] */
  override def compute(split: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val iter = new Iterator[String] {
      logInfo("Input partition: " + split)

      val metrics: SgdcRDDMetrics = new SgdcRDDMetrics(context)

      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false

      val outputStream: Try[ByteArrayOutputStream] = Retry(maxRetries) {
        downloadSlot(split.asInstanceOf[SgdcRDDPartition[A]].slot)
      }

      var reader: BufferedReader = null

      outputStream match {
        case Success(v) => {
          reader = new BufferedReader(new InputStreamReader(
            IOUtils.parseInputStream(new ByteArrayInputStream(v.toByteArray))
          ))
        }
        case Failure(e) => {
          val msg = s"Error downloading partition[$split] after $maxRetries tries"
          logError(msg)

          throw SgdcDownloaderException(msg, e)
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
    * Internal method that download a es.alvsanand.sgdc.core.downloader.SgdcSlot using the
    * specific es.alvsanand.sgdc.core.downloader.SgdcDownloader. Due to the unknown nature of the
    * es.alvsanand.sgdc.core.downloader.SgdcDownloader, it cannot be used a shared instance
    * between threads.
 *
    * @param slot The slot to be downloaded
    * @return The data of the slot
    */
  private def downloadSlot(slot: A): ByteArrayOutputStream = {
    logInfo(s"Downloading slot[$slot]")

    val sgdcDownloader = sgdcDownloaderFactory.get(parameters)

    val outputStream = new ByteArrayOutputStream()

    sgdcDownloader.download(slot, outputStream)

    outputStream
  }

  /**
    * Returns the current slots that are going to be processed by this RDD.
    * @return A SgdcRange of slots
    */
  override def range(): SgdcRange = {
    if (slots.isEmpty) {
      SgdcRange.Empty
    }
    else {
      if (slots.head.isInstanceOf[SgdcDateSlot]) {
        SgdcRange(slots.map(_.asInstanceOf[SgdcDateSlot].date).sorted
          .headOption.getOrElse(null)
          , slots.map(_.name).toSeq)
      }
      else{
        SgdcRange(slots.map(_.name).toSeq)
      }
    }
  }

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD]] */
  override protected def getPartitions: Array[Partition] = {
    slots.zipWithIndex.map { case (f, i) => new SgdcRDDPartition(f, i) }
  }
}

/**
  * This trait represent an object which has a org.apache.spark.streaming.sgdc.SgdcRange.
  */
trait HasSgdcFileRange {
  /**
    * Return the current SgdcRange
    * @return A SgdcRange
    */
  def range: SgdcRange
}
