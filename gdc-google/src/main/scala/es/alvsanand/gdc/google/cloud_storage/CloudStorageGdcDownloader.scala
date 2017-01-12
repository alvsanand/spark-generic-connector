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
import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException, GdcDownloaderParameters, GdcFile}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.google.GoogleHelper

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class CloudStorageFile(file: String, date: Option[Date] = None) extends GdcFile

case class CloudStorageParameters(credentialsZipPath: String, bucket: String)
  extends GdcDownloaderParameters

/**
  * Created by alvsanand on 30/09/16.
  */
private[cloud_storage]
class CloudStorageGdcDownloader(parameters: CloudStorageParameters)
  extends GdcDownloader[CloudStorageFile, CloudStorageParameters](parameters) {

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

  private def initClient(): Storage = {
    logDebug(s"Initiating DoubleClickDataTransferDownloader[$parameters]")

    val tmpDir = IOUtils.createTempDirectory()
    sys.addShutdownHook {IOUtils.deleteDirectory(tmpDir)}
    IOUtils.unzipToFileDirectory(parameters.credentialsZipPath, tmpDir.getPath)

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

  def list(): Seq[CloudStorageFile] = {
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

      files.map(x => CloudStorageFile(x.getName, Option(new Date(x.getTimeCreated.getValue))))
        .sortBy(_.file).toSeq
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

  def download(file: CloudStorageFile, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading file[$file] of bucket[${parameters.bucket}]")

      val getObject = client.objects().get(parameters.bucket, file.file)

      getObject.executeMediaAndDownloadTo(out)

      logDebug(s"Downloaded file[${parameters.bucket}] of bucket[${parameters.bucket}]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        val msg = s"Error downloading file[${parameters.bucket}] of bucket[${parameters.bucket}]"
        logError(msg, e)
        throw GdcDownloaderException(msg, e)
      }
    }
  }
}
