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

package org.apache.spark.rdd.gdc

import java.util.Date

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.util.gdc.{DownloadFile, DownloaderFactory, Retry}
import org.apache.spark.{Logging, SparkContext}

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * Created by alvsanand on 22/11/16.
  */
@DeveloperApi
case class DownloadContext(@transient sc: SparkContext) extends Logging {

  def createDownloadRDD[A <: DownloadFile : ClassTag](downloaderFactory: DownloaderFactory[A],
                                                      downloaderParams: Map[String, String],
                                                      fromDate: Option[Date] = None,
                                                      toDate: Option[Date] = None,
                                                      charset: String = "UTF-8",
                                                      maxRetries: Int = 2
                                                     ): RDD[String] = {
    val downloader = downloaderFactory.get(downloaderParams)

    Retry(maxRetries + 1) {
      downloader.list()
    } match {
      case Success(list) => {
        val files: Array[A] = list.flatMap(f => filterFile(f, fromDate, toDate))
          .sortWith { case (a, b) => a.compareTo(b) < 0 }
          .toArray[A]

        logInfo(s"Detected files[${files.mkString(",")}] to process")

        if (files.length == 0) {
          sc.emptyRDD[String]
        }
        else {
          new DownloadRDD[A](sc, files, downloaderFactory, downloaderParams, maxRetries = maxRetries)
        }
      }
      case Failure(e) => {
        logError(s"Error getting files to process", e)
        sc.emptyRDD[String]
      }
    }
  }

  private def filterFile[A <: DownloadFile](downloadFile: A,
                                            fromDate: Option[Date],
                                            toDate: Option[Date]): Option[A] = {
    if ((fromDate.isDefined || toDate.isDefined) && downloadFile.date.isEmpty) {
      logDebug(s"$downloadFile is rejected because it needs date")
      return None
    }

    //Inclusive
    if (fromDate.isDefined && fromDate.get.getTime > downloadFile.date.get.getTime) {
      logDebug(s"$downloadFile is rejected because date[${downloadFile.date}] is less than $fromDate")
      return None
    }

    //Exclusive
    if (toDate.isDefined && toDate.get.getTime <= downloadFile.date.get.getTime) {
      logDebug(s"$downloadFile is rejected because date[${downloadFile.date}] is equal or greater than $toDate")
      return None
    }

    return Option(downloadFile)
  }
}
