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

package es.alvsanand.gdc.google

import java.io.{ByteArrayInputStream, File, OutputStream, StringReader}
import java.nio.file.{Files, Path, Paths}

import com.google.api.client.extensions.java6.auth.oauth2.{AuthorizationCodeInstalledApp, VerificationCodeReceiver}
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.storage.{Storage, StorageScopes}
import es.alvsanand.gdc.core.downloader.GdcDownloaderException
import es.alvsanand.gdc.core.util.{IOUtils, Logging}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}


/**
  * This object eases the task of creating authenticate into
  * [[https://cloud.google.com/storage/ Google Cloud Storage]]. The Google authentication method
  * used by this implementation is Authorization code flow using a GooglePromptReceiver. See
  * [[https://developers.google.com/api-client-library/java/google-api-java-client/oauth2
  *  Authorization Code Flow]] for more info.
  *
  *  This is is the way to do it:
  *
  *  - Open a Spark Scala Shell or Scala Shell.
  *  - Execute GoogleHelper.createCredentialsZip() passing the JSON of the OAuth 2.0 client ID and
  *  the desired url to store the credentials.
  *  - GoogleHelper library will print a URL that you have to visit using a Browser.
  *  - Follow the instructions of the Google Page in order to obtain the authentication token.
  *  - Paste the token in the Shell.
  *  - The helper will create the zip and upload if the url is a HDFS file.
  *
  *  Note: it is recommended to use a HDFS path like [[hdfs://127.0.0.1/credentials.zip]] in order
  *  to store the credentials in a distributed way. If no ever machine of the Spark cluster must
  *  have a copy of this file.
  */
object GoogleHelper extends Logging {
  val CLIENT_SECRET_FILE: String = "client_secrets.json"
  val STOREDCREDENTIAL_FILE: String = "StoredCredential"

  private val REQUIRED_CREDENTIALS_FILES: Array[String] =
    Array(CLIENT_SECRET_FILE, STOREDCREDENTIAL_FILE).sorted


  /**
    * Check if the directory contains the valid Google credential files.
    * @param directory The directory that contains the credentials.
    */
  def checkCredentialsDirectory(directory: String): Unit = {
    checkCredentialsDirectory(new File(directory))
  }

  /**
    * Check if the directory contains the valid Google credential files.
    * @param directory The directory that contains the credentials.
    */
  def checkCredentialsDirectory(directory: File): Unit = {
    val directoryPath = Paths.get(directory.getPath)

    val currentFiles = Files.list(directoryPath).iterator().asScala
      .map{ x => x.asInstanceOf[Path].toFile.getName}.toArray.sorted

    if(!REQUIRED_CREDENTIALS_FILES.forall(currentFiles.contains(_))) {
      val msg = s"Error checking Credentials files[" +
        s"${REQUIRED_CREDENTIALS_FILES.mkString(",")}] are required"

      throw GdcDownloaderException(msg)
    }
  }

  /**
    * Create a Credentials zip used by
    * es.alvsanand.gdc.google.cloud_storage.CloudStorageGdcDownloader and
    * es.alvsanand.gdc.google.dcm_data_transfer.DataTransferGdcDownloader
    * @param json The json of the OAuth 2.0 client ID
    * @param outputUrl The url of the credentials zip. It can be a local file [/tmp/credentials.zip]
    *                  or a HDFS url [[hdfs://127.0.0.1/credentials.zip]]
    * @return Success if the execution is correct. Failure if note.
    */
  def createCredentialsZip(json: String, outputUrl: String): Try[Unit] = {
    val tmpDir = IOUtils.createTempDirectory()

    Try {
      val JSON_FACTORY: JsonFactory = JacksonFactory.getDefaultInstance()
      val httpTransport = GoogleNetHttpTransport.newTrustedTransport()

      val dataStoreFactory = new FileDataStoreFactory(tmpDir)

      val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new StringReader(json))

      val flow: GoogleAuthorizationCodeFlow =
        new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY,
          clientSecrets, StorageScopes.all())
          .setDataStoreFactory(dataStoreFactory).build
      val receiver: VerificationCodeReceiver = new GooglePromptReceiver
      val credential = new AuthorizationCodeInstalledApp(flow, receiver).authorize("user")

      val client: Storage = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
        .setApplicationName("GoogleHelper").build()

      client.buckets()

      var out: OutputStream = null

      try {
        out = IOUtils.getOutputStream((new File(tmpDir, CLIENT_SECRET_FILE)).getPath)

        IOUtils.copy(new ByteArrayInputStream(json.getBytes), out)

        IOUtils.zip(tmpDir.getPath, outputUrl)
      }
      finally {
        if (out != null) {
          out.close()
        }

        IOUtils.deleteDirectory(tmpDir.getPath)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    checkCredentialsDirectory("/home/alvsanand/tmp/aaa")
  }
}
