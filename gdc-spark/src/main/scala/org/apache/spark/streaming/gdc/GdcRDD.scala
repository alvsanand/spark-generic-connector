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

import java.io._
import java.util.concurrent.atomic.AtomicLong

import es.alvsanand.gdc.core.downloader._
import es.alvsanand.gdc.core.util.{IOUtils, Retry}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ShutdownHookManager

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

/**
  * An RDD that provides core functionality for reading es.alvsanand.gdc.core.downloader.GdcSlot
  * retrieved by es.alvsanand.gdc.core.downloader.GdcDownloader.
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
  * @param slots The list of slots
  * @param gdcDownloaderFactory The GdcDownloaderFactory used to create the
  *                             es.alvsanand.gdc.core.downloader.GdcDownloader.
  * @param parameters The parameters of the es.alvsanand.gdc.core.downloader.GdcDownloader.
  * @param charset The java.nio.charset.Charset name of the slots that are going to be
  *                downloaded.
  * @param maxRetries The maximum number times that an operation of a
  *                   es.alvsanand.gdc.core.downloader.GdcDownloader is going to be repeated
  *                   in case of failure.
  * @tparam A The type of es.alvsanand.gdc.core.downloader.GdcSlot
  * @tparam B The type of es.alvsanand.gdc.core.downloader.GdcDownloaderParameters
  */
private[gdc]
class GdcRDD[A <: GdcSlot: ClassTag, B <: GdcDownloaderParameters: ClassTag](sc: SparkContext,
                                                                             slots: Array[A],
                                                                             gdcDownloaderFactory: GdcDownloaderFactory[A, B],
                                                                             parameters: B,
                                                                             charset: String = "UTF-8",
                                                                             maxRetries: Int = 3
                                     ) extends RDD[String](sc, Nil) with
  HasGdcFileRange {

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD]] */
  override def compute(split: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val iter = new Iterator[String] {
      logInfo("Input partition: " + split)

      val metrics: GdcRDDMetrics = new GdcRDDMetrics(context)

      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false

      val outputStream: Try[ByteArrayOutputStream] = Retry(maxRetries) {
        downloadSlot(split.asInstanceOf[GdcRDDPartition[A]].slot)
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

          throw GdcDownloaderException(msg, e)
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
    * Internal method that download a es.alvsanand.gdc.core.downloader.GdcSlot using the
    * specific es.alvsanand.gdc.core.downloader.GdcDownloader. Due to the unknown nature of the
    * es.alvsanand.gdc.core.downloader.GdcDownloader, it cannot be used a shared instance
    * between threads.
 *
    * @param slot The slot to be downloaded
    * @return The data of the slot
    */
  private def downloadSlot(slot: A): ByteArrayOutputStream = {
    logInfo(s"Downloading slot[$slot]")

    val gdcDownloader = gdcDownloaderFactory.get(parameters)

    val outputStream = new ByteArrayOutputStream()

    gdcDownloader.download(slot, outputStream)

    outputStream
  }

  /**
    * Returns the current slots that are going to be processed by this RDD.
    * @return A GdcRange of slots
    */
  override def range(): GdcRange = {
    if (slots.isEmpty) {
      GdcRange.Empty
    }
    else {
      if (slots.head.isInstanceOf[GdcDateSlot]) {
        GdcRange(slots.map(_.asInstanceOf[GdcDateSlot].date).sorted
          .headOption.getOrElse(null)
          , slots.map(_.name).toSeq)
      }
      else{
        GdcRange(slots.map(_.name).toSeq)
      }
    }
  }

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD]] */
  override protected def getPartitions: Array[Partition] = {
    slots.zipWithIndex.map { case (f, i) => new GdcRDDPartition(f, i) }
  }
}

/**
  * This trait represent an object which has a org.apache.spark.streaming.gdc.GdcRange.
  */
trait HasGdcFileRange {
  /**
    * Return the current GdcRange
    * @return A GdcRange
    */
  def range: GdcRange
}
