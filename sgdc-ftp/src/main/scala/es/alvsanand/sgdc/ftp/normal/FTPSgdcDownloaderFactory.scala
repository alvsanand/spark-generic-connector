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

package es.alvsanand.sgdc.ftp.normal

import es.alvsanand.sgdc.core.downloader.{SgdcDownloader, SgdcDownloaderFactory}
import es.alvsanand.sgdc.ftp.FTPSlot

/**
  * This Factory creates instances of es.alvsanand.sgdc.core.downloader.SgdcDownloader for
  * integrating with a [[https://en.wikipedia.org/wiki/File_Transfer_Protocol FTP server]]. It
  * list and download all the files that are in a configured directory.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The FTP client will authenticate using the credentials.
  *
  *  - If the proxy parameters are set, the FTP client will use a proxy instead of a direct
  * connection.
  */
object FTPSgdcDownloaderFactory extends SgdcDownloaderFactory[FTPSlot, FTPParameters] {

  /**
    * Method that returns a new instance of a SgdcDownloader
    * @param parameters The parameters of the SgdcDownloader
    * @return A new instance of a SgdcDownloader.
    */
  override def get(parameters: FTPParameters): SgdcDownloader[FTPSlot, FTPParameters] = {
    new FTPSgdcDownloader(parameters)
  }
}
