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

import com.google.api.client.extensions.java6.auth.oauth2.{AuthorizationCodeInstalledApp,
VerificationCodeReceiver}
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow,
GoogleClientSecrets}
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.storage.{Storage, StorageScopes}
import es.alvsanand.gdc.core.util.{IOUtils, Logging}

import scala.util.{Failure, Try}

/**
  * Created by alvsanand on 12/01/17.
  */
object GoogleHelper extends Logging {
  val CLIENT_SECRET_FILE: String = "client_secrets.json"

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
}
