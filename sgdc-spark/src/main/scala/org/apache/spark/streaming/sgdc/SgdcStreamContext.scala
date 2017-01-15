/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

import es.alvsanand.sgdc.core.downloader.{SgdcDownloaderFactory, SgdcDownloaderParameters, SgdcDateSlot}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag

/**
  * SgdcStreamContext creates a InputDStreams which is able to read slots using a
  * es.alvsanand.sgdc.core.downloader.SgdcDownloader. When a new slots comes, it creates a
  * org.apache.spark.streaming.sgdc.SgdcRDD. The way it works as follows.
  *
  * At each batch interval, the es.alvsanand.sgdc.core.downloader.SgdcDownloader list all its
  * available slots and detects if there are new slots which will be for that batch. For this
  * purpose, this class remembers the information about the slots selected in past batches for
  * a certain duration
  *
  * This makes some assumptions from the other system that
  * es.alvsanand.sgdc.core.downloader.SgdcDownloader is monitoring.
  *
  *  - The clock of the name system is assumed to synchronized with the clock of the machine running
  *    the streaming app.
  *  - The es.alvsanand.sgdc.core.downloader.SgdcDateSlot returned by the
  *  es.alvsanand.sgdc.core.downloader.SgdcDownloader should return a date because if not it will
  *  be sorted by name.
  *  
  * @param ssc The StreamingContext
  */
case class SgdcStreamContext(@transient ssc: StreamingContext) {

  /**
    * * Create a org.apache.spark.streaming.sgdc.SgdcInputDStream from a SparkContext using a
    * es.alvsanand.sgdc.core.downloader.SgdcDownloaderFactory and some parameters.
 *
    * @param sgdcDownloaderFactory The SgdcDownloaderFactory used to create the
    *                             es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param parameters The parameters of the es.alvsanand.sgdc.core.downloader.SgdcDownloader.
    * @param fromSgdcRange The range from where the SgdcInputDStream must begin.
    * @param charset The java.nio.charset.Charset name of the slots that are going to be
    *                downloaded.
    * @param maxRetries  The maximum number times that an operation of a
    *                   es.alvsanand.sgdc.core.downloader.SgdcDownloader is going to be repeated
    *                   in case of failure.
    * @tparam A The type of es.alvsanand.sgdc.core.downloader.SgdcDateSlot
    * @tparam B The type of es.alvsanand.sgdc.core.downloader.SgdcDownloaderParameters
    * @return A SgdcInputDStream
    */
  def createSgdcInputDStream[A <: SgdcDateSlot: ClassTag, B <: SgdcDownloaderParameters: ClassTag]
                                                  (sgdcDownloaderFactory: SgdcDownloaderFactory[A, B],
                                                   parameters: B,
                                                   fromSgdcRange: Option[SgdcRange] = None,
                                                   charset: String = "UTF-8",
                                                   maxRetries: Int = 3): InputDStream[String] =
    new SgdcInputDStream[A, B](ssc, sgdcDownloaderFactory, parameters,
      fromSgdcRange, charset, maxRetries)
}
