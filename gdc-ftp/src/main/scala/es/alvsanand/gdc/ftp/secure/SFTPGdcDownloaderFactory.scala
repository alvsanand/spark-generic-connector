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

package es.alvsanand.gdc.ftp.secure

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderFactory}
import es.alvsanand.gdc.ftp.FTPSlot

/**
  * This Factory creates instances of es.alvsanand.gdc.core.downloader.GdcDownloader for
  * integrating with a [[https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol SFTP server]]. It
  * list and download all the files that are in a configured directory.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The SFTP client will authenticate using the credentials.
  *  - If the keystore is set, the SFTP client will use the private key to authenticate instead of
  *  the password.
  */
object SFTPGdcDownloaderFactory extends GdcDownloaderFactory[FTPSlot, SFTPParameters] {

  /**
    * Method that returns a new instance of a GdcDownloader
    * @param parameters The parameters of the GdcDownloader
    * @return A new instance of a GdcDownloader.
    */
  override def get(parameters: SFTPParameters): GdcDownloader[FTPSlot, SFTPParameters] = {
    new SFTPGdcDownloader(parameters)
  }
}
