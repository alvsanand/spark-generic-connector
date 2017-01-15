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

import java.io._
import java.util.Date

import com.google.api.client.extensions.java6.auth.oauth2.{AuthorizationCodeInstalledApp, VerificationCodeReceiver}
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.storage.model.{Objects, StorageObject}
import com.google.api.services.storage.{Storage, StorageScopes}
import com.wix.accord.Validator
import com.wix.accord.dsl._
import es.alvsanand.gdc.core.downloader._
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.google.GoogleHelper

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * The es.alvsanand.gdc.core.downloader.GdcSlot implementation for
  * es.alvsanand.gdc.google.cloud_storage.CloudStorageGdcDownloader.
  *
  * Note: every CloudStorageSlot corresponds to a file in Google Cloud Storage
  * @param name The name of the file
  * @param date The creation date of the file
  */
case class CloudStorageSlot(name: String, date: Date) extends GdcDateSlot

/**
  * The parameters for es.alvsanand.gdc.google.cloud_storage.CloudStorageGdcDownloader.
  * @param credentialsZipPath The path to the credentials zip file [Obligatory]. See
  *                           es.alvsanand.gdc.google.GoogleHelper to know more about the
  *                           credentials zip file.
  * @param bucket The bucket path where the downloader will find files [Obligatory].
  */
case class CloudStorageParameters(credentialsZipPath: String, bucket: String)
  extends GdcDownloaderParameters

/**
  * This is a [[https://cloud.google.com/storage/ Google Cloud Storage]] implementation of
  * es.alvsanand.gdc.core.downloader.GdcDownloader. It list and download all the files that are in
  * a configured bucket.
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
  * @param parameters The parameters of the GdcDownloader
  */
private[cloud_storage]
class CloudStorageGdcDownloader(parameters: CloudStorageParameters)
  extends GdcDownloader[CloudStorageSlot, CloudStorageParameters](parameters) {

  /** @inheritdoc */
  override def getValidator(): Validator[CloudStorageParameters] = {
    validator[CloudStorageParameters] { p =>
      p.bucket is notNull
      p.bucket is notEmpty
      p.credentialsZipPath is notNull
      p.credentialsZipPath is notEmpty
    }
  }

  private val APPLICATION_NAME: String = "CloudStorageDownloader"

  private var _client: Storage = null
  private def client(): Storage = synchronized {
    if (_client == null){
      _client = initClient()
    }

    _client
  }

  /**
    * Method that initialize the Google Cloud Storage client.
    * @return The  Google Cloud Storage client.
    */
  private def initClient(): Storage = {
    logDebug(s"Initiating DoubleClickDataTransferDownloader[$parameters]")

    val tmpDir = IOUtils.createTempDirectory()
    sys.addShutdownHook {IOUtils.deleteDirectory(tmpDir)}
    IOUtils.unzipToFileDirectory(parameters.credentialsZipPath, tmpDir.getPath)

    GoogleHelper.checkCredentialsDirectory(tmpDir)

    val dataStoreDir = tmpDir

    val JSON_FACTORY: JsonFactory = JacksonFactory.getDefaultInstance()
    val httpTransport = GoogleNetHttpTransport.newTrustedTransport()

    val dataStoreFactory = new FileDataStoreFactory(dataStoreDir)

    val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
      new FileReader(new File(tmpDir, GoogleHelper.CLIENT_SECRET_FILE)))

    val flow: GoogleAuthorizationCodeFlow =
      new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY,
        clientSecrets, StorageScopes.all())
        .setDataStoreFactory(dataStoreFactory).build
    val receiver: VerificationCodeReceiver = new GooglePromptReceiver
    val credential = new AuthorizationCodeInstalledApp(flow, receiver).authorize("user")

    val client: Storage = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
      .setApplicationName(APPLICATION_NAME).build()

    logDebug(s"Initiated DoubleClickDataTransferDownloader[$parameters]")

    client
  }

  /** @inheritdoc */
  @throws(classOf[GdcDownloaderException])
  override def list(): Seq[CloudStorageSlot] = {
    var files: Array[StorageObject] = Array.empty

    Try({
      logDebug(s"Listing files of bucket[${parameters.bucket}]")

      val listObjects = client.objects().list(parameters.bucket)

      var objects: Objects = null
      do {
        objects = listObjects.execute()

        val items = objects.getItems().asScala

        if (items != null) {
          files ++= items
        }

        listObjects.setPageToken(objects.getNextPageToken())
      } while (objects.getNextPageToken() != null)

      logDebug(s"Listed files of bucket[${parameters.bucket}]: [${files.mkString(",")}]")

      files.map(x => CloudStorageSlot(x.getName, new Date(x.getTimeCreated.getValue)))
        .sortBy(_.name).toSeq
    })
    match {
      case Success(v) => v
      case Failure(e) => {
        val msg = s"Error listing files of bucket[${parameters.bucket}]: $files"
        logError(msg, e)
        throw GdcDownloaderException(msg, e)
      }
    }
  }

  /** @inheritdoc */
  @throws(classOf[GdcDownloaderException])
  override def download(slot: CloudStorageSlot, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading name[$slot] of bucket[${parameters.bucket}]")

      val getObject = client.objects().get(parameters.bucket, slot.name)

      getObject.executeMediaAndDownloadTo(out)

      logDebug(s"Downloaded name[${parameters.bucket}] of bucket[${parameters.bucket}]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        val msg = s"Error downloading name[${parameters.bucket}] of bucket[${parameters.bucket}]"
        logError(msg, e)
        throw GdcDownloaderException(msg, e)
      }
    }
  }
}
