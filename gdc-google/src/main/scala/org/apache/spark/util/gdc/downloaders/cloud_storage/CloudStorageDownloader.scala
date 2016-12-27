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

package org.apache.spark.util.gdc.downloaders.dcm_data_transfer

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
import org.apache.spark.util.gdc.Downloader
import org.apache.spark.util.gdc.downloaders.cloud_storage.CloudStorageFile

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


/**
  * Created by alvsanand on 30/09/16.
  */
class CloudStorageDownloader(credentialsPath: String, bucket: String) extends Downloader[CloudStorageFile] {
  if (credentialsPath == null || credentialsPath == "") throw new IllegalArgumentException(s"credentialsPath cannot be empty")
  if (bucket == null || bucket == "") throw new IllegalArgumentException(s"bucket cannot be empty")

  private val APPLICATION_NAME: String = "CloudStorageDownloader"

  private val CLIENT_SECRET_FILE: String = "client_secrets.json"

  private var _builder: Storage = null

  def list(): Seq[CloudStorageFile] = {
    var files: Array[StorageObject] = Array.empty

    Try({
      logDebug(s"Listing files of bucket[$bucket]")

      val listObjects = builder.objects().list(bucket)

      var objects: Objects = null
      do {
        objects = listObjects.execute()

        val items = objects.getItems().asScala

        if (items != null) {
          files ++= items
        }

        listObjects.setPageToken(objects.getNextPageToken())
      } while (objects.getNextPageToken() != null)

      logDebug(s"Listed files of bucket[$bucket]: [${files.mkString(",")}]")

      files.map(x => CloudStorageFile(x.getName, Option(new Date(x.getTimeCreated.getValue)))).toSeq
    })
    match {
      case Success(v) => v
      case Failure(e) => logError(s"Error listing files of bucket[$bucket]: $files", e); throw e
    }
  }

  private def builder(): Storage = synchronized {
    if (_builder == null) {
      logInfo(s"Initiating CloudStorageDownloader[credentialsPath: $credentialsPath, bucket: $bucket]")

      val dataStoreDir = new File(credentialsPath)

      val JSON_FACTORY: JsonFactory = JacksonFactory.getDefaultInstance()
      val httpTransport = GoogleNetHttpTransport.newTrustedTransport()

      logDebug("#################")
      logDebug(sys.props.toArray.sorted.mkString("\n"))
      logDebug("#################")
      logDebug(s"dataStoreDir[$dataStoreDir -> canRead: ${dataStoreDir.canRead}, canWrite: ${dataStoreDir.canWrite}, canExecute: ${dataStoreDir.canExecute}]")
      logDebug("#################")

      val dataStoreFactory = new FileDataStoreFactory(dataStoreDir)

      val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new FileReader(new File(credentialsPath, CLIENT_SECRET_FILE)))

      val flow: GoogleAuthorizationCodeFlow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets, StorageScopes.all())
        .setDataStoreFactory(dataStoreFactory).build
      val receiver: VerificationCodeReceiver = new GooglePromptReceiver
      val credential = new AuthorizationCodeInstalledApp(flow, receiver).authorize("user")

      _builder = new Storage.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build()

      logInfo(s"Initiated CloudStorageDownloader[credentialsPath: $credentialsPath, bucket: $bucket]")
    }

    _builder
  }

  def downloadFile(file: CloudStorageFile, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading file[$file] of bucket[$bucket]")

      val getObject = builder.objects().get(bucket, file.file)

      getObject.executeMediaAndDownloadTo(out)

      logDebug(s"Downloaded file[$bucket] of bucket[$bucket]")
    })
    match {
      case Success(v) =>
      case Failure(e) => logError(s"Error downloading file[$bucket] of bucket[$bucket]", e); throw e
    }
  }
}

object CloudStorageDownloader {

  def main(args: Array[String]): Unit = {
    // /home/alvsanand/tmp dcdt_-dcm_account[0-9]+ dcm_account[0-9]+_activity_20160922_20160923_042948_291626301.csv.gz
    val credentialFile = args(0)
    val bucket = args(1)
    val file = args(2)

    val downloader = new CloudStorageDownloader(credentialFile, bucket)
    downloader.list().foreach(println)
    downloader.downloadFile(CloudStorageFile(args(2)), new FileOutputStream(File.createTempFile(file, ".tmp")))
  }
}