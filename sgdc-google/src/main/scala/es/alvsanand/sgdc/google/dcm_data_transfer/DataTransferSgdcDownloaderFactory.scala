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

package es.alvsanand.sgdc.google.dcm_data_transfer

import es.alvsanand.sgdc.core.downloader.{SgdcDownloader, SgdcDownloaderFactory}

/**
  * This Factory creates a instances of es.alvsanand.sgdc.core.downloader.SgdcDownloader for
  * integrating with
  *  [[https://support.google.com/dcm/partner/answer/165589?hl=en DoubleClick Data Transfer]].
  *  It list and download all the
  *  files that are in a configured bucket.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The CloudStorage client will authenticate using the credentials zip. See
  *                           es.alvsanand.sgdc.google.GoogleHelper to know more about the
  *                           credentials zip file.
  *
  *  - The Google authentication method used by this implementation is Authorization code flow using
  * a GooglePromptReceiver. See
  *  [[https://developers.google.com/api-client-library/java/google-api-java-client/oauth2
  *  Authorization Code Flow]] for more info.
  *
  *  - The SgdcDownloader is able to filter the type of Data Transfer files.
  */
object DataTransferSgdcDownloaderFactory
    extends SgdcDownloaderFactory[DataTransferSlot, DataTransferParameters] {

  /**
    * Method that returns a new instance of a SgdcDownloader
    * @param parameters The parameters of the SgdcDownloader
    * @return A new instance of a SgdcDownloader.
    */
  override def get(parameters: DataTransferParameters):
      SgdcDownloader[DataTransferSlot, DataTransferParameters] = {
    new DataTransferSgdcDownloader(parameters)
  }
}
