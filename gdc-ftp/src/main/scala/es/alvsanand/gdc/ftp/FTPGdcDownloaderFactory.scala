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

package es.alvsanand.gdc.ftp

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderFactory}

/**
  * Created by alvsanand on 10/12/16.
  */

object FTPGdcDownloaderFactory extends GdcDownloaderFactory[FTPFile] {
  private val DEFAULT_FTP_PORT = "21"

  private val NOT_CLIENT_PARAMETERS = Set("host", "user", "port", "password", "directory")

  def get(gdcDownloaderParams: Map[String, String]): GdcDownloader[FTPFile] = {
    new FTPGdcDownloader(
      gdcDownloaderParams.get("host").getOrElse(""),
      gdcDownloaderParams.get("port").getOrElse(DEFAULT_FTP_PORT).toInt,
      gdcDownloaderParams.get("user").getOrElse(""),
      gdcDownloaderParams.get("password").getOrElse(""),
      gdcDownloaderParams.get("directory").getOrElse(""),
      gdcDownloaderParams.filterNot {
        case (k, v) => NOT_CLIENT_PARAMETERS.contains(k)
      }
    )
  }
}
