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

package es.alvsanand.gdc.google.cloud_storage

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderFactory}

/**
  * This Factory creates instances of es.alvsanand.gdc.core.downloader.GdcDownloader for
  * integrating with [[https://cloud.google.com/storage/ Google Cloud Storage]]. It list and
  * download all the files that are in a configured bucket.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The CloudStorage client will authenticate using the credentials zip. See
  *                           es.alvsanand.gdc.google.GoogleHelper to know more about the
  *                           credentials zip file.
  *
  *  - The Google authentication method used by this implementation is Authorization code flow using
  * a GooglePromptReceiver. See
  *  [[https://developers.google.com/api-client-library/java/google-api-java-client/oauth2
  *  Authorization Code Flow]] for more info.
  */
object CloudStorageGdcDownloaderFactory
    extends GdcDownloaderFactory[CloudStorageSlot, CloudStorageParameters] {

  /**
    * Method that returns a new instance of a GdcDownloader
    * @param parameters The parameters of the GdcDownloader
    * @return A new instance of a GdcDownloader.
    */
  override def get(parameters: CloudStorageParameters):
      GdcDownloader[CloudStorageSlot, CloudStorageParameters] = {
    new CloudStorageGdcDownloader(parameters)
  }
}
