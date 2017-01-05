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

package es.alvsanand.gdc.google.dcm_data_transfer

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
import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderParameters, GdcFile}
import es.alvsanand.gdc.google.dcm_data_transfer.DataTransferFileTypes.DataTransferFileType

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class DataTransferFile(file: String, date: Option[Date] = None,
                            ddtFileType: Option[DataTransferFileType] = None) extends GdcFile

case class DataTransferParameters(credentialsPath: String, bucket: String,
                                  types: Seq[DataTransferFileType] = Seq.empty)
  extends GdcDownloaderParameters

/**
  * Created by alvsanand on 30/09/16.
  */
private[dcm_data_transfer]
class DataTransferGdcDownloader(parameters: DataTransferParameters)
  extends GdcDownloader[DataTransferFile, DataTransferParameters](parameters) {

  override def getValidator(): Validator[DataTransferParameters] = {
    validator[DataTransferParameters] { p =>
      p.bucket is notNull
      p.bucket is notEmpty
      p.credentialsPath is notNull
      p.credentialsPath is notEmpty
    }
  }

  private val APPLICATION_NAME: String = "DoubleClickDataTransferDownloader"

  private val CLIENT_SECRET_FILE: String = "client_secrets.json"

  private var _builder: Storage = null

  private def builder(): Storage = synchronized {
    if (_builder == null) {
      logDebug(s"Initiating DoubleClickDataTransferDownloader[$parameters]")

      val dataStoreDir = new File(parameters.credentialsPath)

      val JSON_FACTORY: JsonFactory = JacksonFactory.getDefaultInstance()
      val httpTransport = GoogleNetHttpTransport.newTrustedTransport()

      logDebug("#################")
      logDebug(sys.props.toArray.sorted.mkString("\n"))
      logDebug("#################")
      logDebug(s"dataStoreDir[$dataStoreDir -> canRead: ${dataStoreDir.canRead}, canWrite: " +
        s"${dataStoreDir.canWrite}, " +
        s"canExecute: ${dataStoreDir.canExecute}]")
      logDebug("#################")

      val dataStoreFactory = new FileDataStoreFactory(dataStoreDir)

      val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
        new FileReader(new File(parameters.credentialsPath, CLIENT_SECRET_FILE)))

      val flow: GoogleAuthorizationCodeFlow =
        new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY,
          clientSecrets, StorageScopes.all())
          .setDataStoreFactory(dataStoreFactory).build
      val receiver: VerificationCodeReceiver = new GooglePromptReceiver
      val credential = new AuthorizationCodeInstalledApp(flow, receiver).authorize("user")

      _builder = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
        .setApplicationName(APPLICATION_NAME).build()

      logDebug(s"Initiated DoubleClickDataTransferDownloader[$parameters]")
    }

    _builder
  }

  def list(): Seq[DataTransferFile] = {
    var files: Array[StorageObject] = Array.empty

    Try({
      logDebug(s"Listing files of bucket[${parameters.bucket}]")

      val listObjects = builder.objects().list(parameters.bucket)

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

      files.flatMap(x => DataTransferFileTypes.getDataTransferFile(x.getName))
        .filter(f =>
          parameters.types.isEmpty || parameters.types.contains(f.ddtFileType.getOrElse(None))
        ).toSeq
    })
    match {
      case Success(v) => v
      case Failure(e) => {
        logError(s"Error listing files of bucket[${parameters.bucket}]: $files", e)
        throw e
      }
    }
  }

  def download(file: DataTransferFile, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading file[$file] of bucket[${parameters.bucket}]")

      val getObject = builder.objects().get(parameters.bucket, file.file)

      getObject.executeMediaAndDownloadTo(out)

      logDebug(s"Downloaded file[${parameters.bucket}] of bucket[$parameters.ucket]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        logError(s"Error downloading file[${parameters.bucket}] of bucket[${parameters.bucket}]", e)
        throw e
      }
    }
  }
}
