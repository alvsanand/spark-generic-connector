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

import java.io._
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException, GdcDownloaderParameters}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.ftp._
import org.apache.commons.net.ftp.{FTPClient, FTPReply, FTPSClient}
import com.wix.accord.Validator
import com.wix.accord.dsl.{be, _}

import scala.util.{Failure, Success, Try}

/**
  * FTPSGdcDownloader parameters.
  *
  * @param host The host of the FTP server
  * @param port The port of the FTP server. Default: 990.
  * @param cred The used to log in the FTP server
  * @param directory The directory path where the downloader will find files
  * @param defaultTimeout the default timeout to use (in ms). Default: 120 seconds.
  * @param dataTimeout The timeout used of the data connection (in ms). Default: 1200 seconds.
  */
case class FTPSParameters(host: String, port: Int = 990, directory: String,
                          cred: Credentials, defaultTimeout: Int = 120000,
                          dataTimeout: Int = 1200000)
  extends GdcDownloaderParameters {
}

private[secure]
class FTPSGdcDownloader(parameters: FTPSParameters)
  extends GdcDownloader[FTPFile, FTPSParameters](parameters) {

  override def getValidator(): Validator[FTPSParameters] = {
    validator[FTPSParameters] { p =>
      p.host is notNull
      p.host is notEmpty
      p.port should be > 0
      p.directory is notNull
      p.directory is notEmpty
      p.cred is notNull
      if(p.cred!=null) p.cred.user is notNull
      if(p.cred!=null) p.cred.user is notEmpty
      p.defaultTimeout should be > 0
      p.dataTimeout should be > 0
    }
  }

  private val client: FTPClient = initClient()

  private def initClient(): FTPClient = synchronized {
    logInfo(s"Initiating FTPDownloader[$parameters]")

    val client: FTPSClient = new FTPSClient(parameters.cred.hasKeystore())

    client.enterLocalPassiveMode()

    client.setDefaultTimeout(parameters.defaultTimeout)
    client.setDataTimeout(parameters.dataTimeout)

    if(parameters.cred.hasKeystore()){
      val ks = KeyStore.getInstance(KeyStore.getDefaultType())
      ks.load(IOUtils.getInputStream(parameters.cred.keystore.get),
        parameters.cred.keystorePassword.getOrElse("").toCharArray)

      val keyManagerFactory = KeyManagerFactory.getInstance("JKS")
      keyManagerFactory.init(ks, null)

      val keyManagers = keyManagerFactory.getKeyManagers()
      client.setKeyManager(keyManagers(0))
    }

    logInfo(s"Initiated FTPDownloader[$parameters]")

    client
  }

  private def connect(): Unit = {
    if (!client.isConnected) {
      logInfo(s"Connecting FTPDownloader[$parameters]")

      client.connect(parameters.host, parameters.port);

      val reply = client.getReplyCode();

      if (!FTPReply.isPositiveCompletion(reply)) {
        throw GdcDownloaderException(s"Error connecting to server: $reply")
      }
      if (!client.login(parameters.cred.user, parameters.cred.password.getOrElse(""))) {
        throw GdcDownloaderException(s"Error logging in with user[${parameters.cred.user}]")
      }

      logInfo(s"Connecting FTPDownloader[$parameters, user: ${parameters.cred.user}]")
    }
  }

  private def disconnect(): Unit = {
    logInfo(s"Disconnecting FTPDownloader[$parameters]")

    if (client.isConnected) {
      client.disconnect()
    }

    logInfo(s"Disconnecting FTPDownloader[$parameters]")
  }

  private def useClient[T](func: () => T): T = {
    Try(connect()) match {
      case Failure(e) => throw e
      case _ =>
    }

    val value = Try(func())

    Try(disconnect()) // Ignore exception in disconnecting

    value match {
      case Success(s) => s
      case Failure(e) => throw e
    }
  }

  def list(): Seq[FTPFile] = {
    var files: Array[org.apache.commons.net.ftp.FTPFile] = Array.empty

    Try({
      logDebug(s"Listing files of directory[${parameters.directory}]")

      files = useClient[Array[org.apache.commons.net.ftp.FTPFile]](() => {
        client.changeWorkingDirectory(parameters.directory)

        client.listFiles(".")
      })

      logDebug(s"Listed files of directory[${parameters.directory}]: [${files.mkString(",")}]")

      files.filter(_.isFile).map(x =>
        FTPFile(x.getName, Option(x.getTimestamp.getTime))
      ).toSeq
    })
    match {
      case Success(v) => v
      case Failure(e) => {
        logError(s"Error listing files of directory[${parameters.directory}]", e)
        throw e
      }
    }
  }

  def download(file: FTPFile, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading file[$file] of directory[${parameters.directory}]")

      val in = useClient[InputStream](() => {
        client.changeWorkingDirectory(parameters.directory)

        client.retrieveFileStream(file.file)
      })

      if (in != null) IOUtils.copy(in, out)

      logDebug(s"Downloaded file[$file] of directory[${parameters.directory}]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        logError(s"Error downloading file[$file] of directory[${parameters.directory}]", e)
        throw e
      }
    }
  }
}
