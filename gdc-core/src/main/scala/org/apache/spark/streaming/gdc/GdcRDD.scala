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
import java.util.zip.GZIPInputStream

import es.alvsanand.gdc.core.downloader.{GdcDownloaderException, GdcDownloaderFactory, GdcFile}
import es.alvsanand.gdc.core.util.Retry
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ShutdownHookManager

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
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

  private val GZIP_MAGIC_LENGTH = 2

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val iter = new Iterator[String] {
      logInfo("Input partition: " + split)

      val count = new AtomicLong

      // Create methods for updating statistics. Obligatory for Spark compatibility
      val (updateBytesRead, updateRecordRead) = {
        val mTM = ru.runtimeMirror(context.taskMetrics.getClass.getClassLoader)
        val mTMImpl = mTM.reflect(context.taskMetrics)

        ru.typeOf[org.apache.spark.executor.TaskMetrics]
          .decl(ru.TermName("getInputMetricsForReadMethod")) match {
          // Spark 1.5.x / 1.6.x
          case sIM: MethodSymbol => {
            val inputMetrics = mTMImpl.reflectMethod(sIM.asMethod)
              .apply(DataReadMethod.Network).asInstanceOf[InputMetrics]

            val mIM = ru.runtimeMirror(inputMetrics.getClass.getClassLoader)
            val mIMImpl = mIM.reflect(inputMetrics)

            val sSBRC = ru.typeOf[InputMetrics].decl(ru.TermName("setBytesReadCallback"))
            mIMImpl.reflectMethod(sSBRC.asMethod)
              .apply(Some(() => count.get().toLong))

            (
              () => {
                Unit
              },
              () => {
                inputMetrics.incRecordsRead(1)
              }
            )
          }
          // Spark 2.x
          case NoSymbol => {
            val sIM = ru.typeOf[org.apache.spark.executor.TaskMetrics]
              .decl(ru.TermName("inputMetrics"))

            val inputMetrics = mTMImpl.reflectField(sIM.asTerm)
              .get.asInstanceOf[InputMetrics]

            val mIM = ru.runtimeMirror(inputMetrics.getClass.getClassLoader)
            val mIMImpl = mIM.reflect(inputMetrics)

            val sSBRC = ru.typeOf[InputMetrics].decl(ru.TermName("setBytesRead"))

            val existingBytesRead = inputMetrics.bytesRead

            (() => {
              mIMImpl.reflectMethod(sSBRC.asMethod)
                .apply(existingBytesRead + count.get().toLong)
            },
              () => {
                inputMetrics.incRecordsRead(1)
              }
            )
          }
        }
      }

      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false

      val outputStream: Try[ByteArrayOutputStream] = Retry(maxRetries + 1) {
        gdcFile(split.asInstanceOf[GdcRDDPartition[A]].gdcFile)
      }

      var reader: BufferedReader = null

      outputStream match {
        case Success(v) => {
          reader = new BufferedReader(new InputStreamReader(
            getInputStream(new ByteArrayInputStream(v.toByteArray))
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

  private def getInputStream(inputStream: InputStream): InputStream = {
    try {
      inputStream.mark(GZIP_MAGIC_LENGTH);

      val bytes = Array.ofDim[Byte](GZIP_MAGIC_LENGTH)

      inputStream.read(bytes)

      inputStream.reset()
      if (bytes(0) == GZIPInputStream.GZIP_MAGIC.toByte && bytes(1) == (GZIPInputStream
        .GZIP_MAGIC >> 8).toByte) {
        new GZIPInputStream(inputStream)
      }
      else {
        inputStream
      }
    } catch {
      case e: Throwable => inputStream
    }
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
