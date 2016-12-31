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

import es.alvsanand.gdc.core.downloader.{GdcDownloaderException, GdcDownloaderFactory, GdcFile}
import es.alvsanand.gdc.core.util.{IOUtils, Retry}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ShutdownHookManager

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

/**
  * Created by alvsanand on 11/10/16.
  */
private[gdc]
class GdcRDD[A <: GdcFile : ClassTag](sc: SparkContext,
                                      files: Array[A],
                                      gdcDownloaderFactory: GdcDownloaderFactory[A],
                                      gdcDownloaderParams: Map[String, String],
                                      charset: String = "UTF-8",
                                      maxRetries: Int = 3
                                     ) extends RDD[String](sc, Nil) with
  HasGdcFileRange {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val iter = new Iterator[String] {
      logInfo("Input partition: " + split)

      val count = new AtomicLong

      // Create methods for updating statistics
      val (updateBytesRead, updateRecordRead) = {
        val inputMetrics = context.taskMetrics.inputMetrics

        val existingBytesRead = inputMetrics.bytesRead

        (
          () => {
            inputMetrics.setBytesRead(existingBytesRead + count.get().toLong)
          },
          () => {
            inputMetrics.incRecordsRead(1)
          }
        )
      }

      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false

      val outputStream: Try[ByteArrayOutputStream] = Retry(maxRetries) {
        gdcFile(split.asInstanceOf[GdcRDDPartition[A]].gdcFile)
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

        if (!finished) {
          updateRecordRead()
          updateBytesRead()
          count.addAndGet(line.length)
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

          updateBytesRead()
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  private def gdcFile(gdcFile: A): ByteArrayOutputStream = {
    logInfo(s"Downloading file[$gdcFile]")

    val gdcDownloader = gdcDownloaderFactory.get(gdcDownloaderParams)

    val outputStream = new ByteArrayOutputStream()

    gdcDownloader.download(gdcFile, outputStream)

    outputStream
  }

  override def range(): GdcRange = GdcRange(files.flatMap(_.date).sorted
    .headOption.getOrElse(null)
    , files.map(_.file): _*)

  override protected def getPartitions: Array[Partition] = {
    files.zipWithIndex.map { case (f, i) => new GdcRDDPartition(f, i) }
  }
}

trait HasGdcFileRange {
  def range: GdcRange
}
