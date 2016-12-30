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

import java.io.{IOException, ObjectInputStream}

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderFactory, GdcFile}
import es.alvsanand.gdc.core.util.Retry
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.gdc.GdcRange.{DateFilesGdcRange, DateGdcRange}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.{Failure, Success}


/**
  * Created by alvsanand on 6/10/16.
  */
private[gdc]
final class GdcInputDStream[A <: GdcFile : ClassTag](
                                                      ssc_ : StreamingContext,
                                                      gdcDownloaderFactory:
                                                      GdcDownloaderFactory[A],
                                                      gdcDownloaderParams: Map[String,
                                                        String],
                                                      fromGdcRange:
                                                      Option[GdcRange] = None,
                                                      charset: String = "UTF-8",
                                                      maxRetries: Int = 3)
  extends InputDStream[String](ssc_) {
  private[streaming] override val checkpointData = new DownloaderInputDStreamCheckpointData
  @transient private var _gdcDownloader: GdcDownloader[A] = null

  // Remember the last 100 batches
  remember(slideDuration * 100)

  @transient private var batchTimeToSelectedFiles =
    new mutable.HashMap[Time, Array[A]] with mutable.SynchronizedMap[Time, Array[A]]

  // Set of files that were selected in the remembered batches
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()

  override def compute(validTime: Time): Option[RDD[String]] = {
    Retry(maxRetries + 1) {
      gdcDownloader.list()
    } match {
      case Success(list) => {
        val newFiles: Array[A] = list.flatMap(f => isNewFile(f))
          .sortWith { case (a, b) => a.compareTo(b) < 0 }
          .toArray

        if (newFiles.length == 0) {
          None
        }
        else {
          logInfo(s"Detected new Files[${newFiles.mkString(",")}] to process")

          batchTimeToSelectedFiles += ((validTime, newFiles))
          recentlySelectedFiles ++= newFiles.map(_.file)

          // One at a time
          val rdd = filesToRDD(newFiles)

          val metadata = Map(
            "files" -> newFiles,
            "gdcDownloaderParams" -> gdcDownloaderParams.toSeq.sorted.mkString(","),
            "fromGdcRange" -> fromGdcRange,
            StreamInputInfo.METADATA_KEY_DESCRIPTION -> newFiles.mkString("\n"))
          val inputInfo = StreamInputInfo(id, rdd.count, metadata)
          ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

          Some(rdd)
        }
      }
      case Failure(e) => {
        val msg = s"Error computing DownloadInputDStream after $maxRetries tries"
        logError(msg)

        throw new org.apache.spark.SparkException(msg, e)
      }
    }
  }

  private def gdcDownloader = {
    if (_gdcDownloader == null) {
      _gdcDownloader = gdcDownloaderFactory.get(gdcDownloaderParams)
    }
    _gdcDownloader
  }

  private def isNewFile(gdcFile: A): Option[A] = {

    // Reject file if it is old
    val validRange: Boolean = fromGdcRange match {
      case Some(r: DateGdcRange) =>
        (r.date == null ||
          (gdcFile.date.isDefined
            && r.date.getTime <= gdcFile.date.get.getTime)
          )
      case Some(r: DateFilesGdcRange) =>
        ((r.files == null || !r.files.contains(gdcFile.file))
          && (r.date == null ||
          (gdcFile.date.isDefined && r.date.getTime <= gdcFile.date.get.getTime))
          )
      case _ => true
    }

    if (!validRange) {
      logDebug(s"$gdcFile rejected because it is not in range[${fromGdcRange.get}]")
      return None
    }

    // Reject file if it was considered earlier
    if (recentlySelectedFiles.contains(gdcFile.file)) {
      logDebug(s"$gdcFile rejected because it has been already considered")
      return None
    }

    logDebug(s"$gdcFile rejected because it has been accepted" +
      s" with mod date ${gdcFile.date.get}")

    return Option(gdcFile)
  }

  private def filesToRDD(files: Array[A]): RDD[String] = {
    new GdcRDD[A](context.sparkContext, files, gdcDownloaderFactory,
      gdcDownloaderParams, charset, maxRetries)
  }

  override def start(): Unit = {
  }

  def stop(): Unit = {
  }

  private[streaming] override def name: String = s"Downloader stream [$id]"

  private[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)

    val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
    batchTimeToSelectedFiles --= oldFiles.keys

    logDebug("Cleared files are:\n" + oldFiles.map(p => (p._1, p._2)).mkString("\n"))
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")

    ois.defaultReadObject()
    generatedRDDs = new HashMap[Time, RDD[String]]()
    batchTimeToSelectedFiles =
      new mutable.HashMap[Time, Array[A]] with mutable.SynchronizedMap[Time, Array[A]]
    recentlySelectedFiles = new mutable.HashSet[String]()
  }

  private[streaming]
  class DownloaderInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    override def update(time: Time) {
      files.clear()
      files ++= batchTimeToSelectedFiles
    }

    override def cleanup(time: Time) {}

    override def restore() {
      files.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t: Time, f: Array[A]) => {
          // Restore the metadata in both files and generatedRDDs
          logDebug(s"Restoring files for time $t +${f.mkString("[", ", ", "]")}")
          batchTimeToSelectedFiles += ((t, f))
          recentlySelectedFiles ++= f.map(_.file)
          generatedRDDs += ((t, filesToRDD(f)))
        }
      }
    }

    private def files = data.asInstanceOf[mutable.HashMap[Time, Array[A]]]

    override def toString: String = {
      "[\n" + files.size + " file sets\n" +
        files.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }

}

