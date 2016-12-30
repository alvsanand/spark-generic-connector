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

package org.apache.spark.streaming.gdc

import es.alvsanand.gdc.core.downloader.{GdcDownloaderFactory, GdcFile}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag

/**
  * Created by alvsanand on 22/11/16.
  */
@DeveloperApi
case class GdcStreamContext(@transient ssc: StreamingContext) {

  def createDownloadStream[A <: GdcFile : ClassTag](gdcDownloaderFactory: GdcDownloaderFactory[A],
                                                    gdcDownloaderParams: Map[String, String],
                                                    fromGdcFileRange: Option[GdcRange] = None,
                                                    charset: String = "UTF-8",
                                                    maxRetries: Int = 3): InputDStream[String] =
    new GdcInputDStream[A](ssc, gdcDownloaderFactory, gdcDownloaderParams,
      fromGdcFileRange, charset, maxRetries)
}
