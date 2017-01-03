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

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.ftp._
import org.apache.commons.net.ftp.{FTPClient, FTPReply, FTPSClient}

import scala.util.{Failure, Success, Try}


/**
  * Created by alvsanand on 30/09/16.
  */

/**
  * FTPS implementation od¡f GdcDownloader.
  *
  * Note: it works in implicit mode.
  *
  * @param host            The host of the FTP server
  * @param _cred     The port of the FTP server
  * @param directory       The directory path where the dowloader will find files
  * @param clientConfigMap FTPClient specific parameters:
  *                        <ul>
  *                        <li>
  *                        <i>defaultTimeout</i>: the default timeout to use (in ms). Default:
  *                        120 seconds.
  *                        </li>
  *                        <li>
  *                        <i>dataTimeout</i>: the timeout used of the data connection (in ms).
  *                        Default: 1200 seconds.
  *                        </li>
  *                        </ul>
  */
class FTPSGdcDownloader(host: String, port: Int, cred: Credentials,
                        directory: String, clientConfigMap: Map[String, String] = Map.empty)
  extends GdcDownloader[FTPFile] {

  private val DEFAULT_TIMEOUT = 120000
  private val DEFAULT_DATA_TIMEOUT = 1200000

  if (host == null || host == "") {
    throw new IllegalArgumentException("host cannot be empty")
  }

  if (port < 0) {
    throw new IllegalArgumentException("port cannot be less than 1")
  }

  if (cred == null) {
    throw new IllegalArgumentException("credentials cannot be empty")
  }

  if (!cred.isInstanceOf[HasUserPassword]) {
    throw new IllegalArgumentException("credentials must be of type UserPasswordCredentials")
  }

  val _credUP = cred.asInstanceOf[HasUserPassword]

  var _credPK: HasPrivateKey = null
  if (cred.isInstanceOf[HasPrivateKey]) {
    _credPK = cred.asInstanceOf[HasPrivateKey]
  }

  if (directory == null || directory == "") {
    throw new IllegalArgumentException("directory cannot be empty")
  }

  private val client: FTPClient = initClient()

  private def initClient(): FTPClient = synchronized {
    logInfo(s"Initiating FTPDownloader[host: $host, user: ${_credUP.user}]")

    val client: FTPSClient = new FTPSClient(_credPK!=null)

    client.enterLocalPassiveMode()

    var defaultTimeout = DEFAULT_DATA_TIMEOUT
    var dataTimeout = DEFAULT_TIMEOUT

    clientConfigMap.foreach { case (k, v) => k match {
        case "defaultTimeout" if v.forall(_.isDigit) => defaultTimeout = v.toInt
        case "dataTimeout" if v.forall(_.isDigit) => dataTimeout = v.toInt
        case _ =>
      }
    }
    client.setDefaultTimeout(defaultTimeout)
    client.setDataTimeout(dataTimeout)

    if(_credPK!=null){
      val ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(IOUtils.getInputStream(_credPK.keystore), _credPK.keystorePassword.toCharArray);

      val keyManagerFactory = KeyManagerFactory.getInstance("JKS");
      keyManagerFactory.init(ks, null);

      val keyManagers = keyManagerFactory.getKeyManagers();
      client.setKeyManager(keyManagers(0))
    }

    logInfo(s"Initiated FTPDownloader[host: $host, user: ${_credUP.user}]")

    client
  }

  private def connect(): Unit = {
    if (!client.isConnected) {
      logInfo(s"Connecting FTPDownloader[host: $host, user: ${_credUP.user}]")

      client.connect(host, port);

      val reply = client.getReplyCode();

      if (!FTPReply.isPositiveCompletion(reply)) {
        throw GdcDownloaderException(s"Error connecting to host[$host]: $reply");
      }
      if (!client.login(_credUP.user, _credUP.password)) {
        throw GdcDownloaderException(s"Error logging in to host[$host] and user[${_credUP.user}]");
      }

      logInfo(s"Connecting FTPDownloader[host: $host, user: ${_credUP.user}]")
    }
  }

  private def disconnect(): Unit = {
    logInfo(s"Disconnecting FTPDownloader[host: $host, user: ${_credUP.user}]")

    if (client.isConnected) {
      client.disconnect()
    }

    logInfo(s"Disconnecting FTPDownloader[host: $host, user: ${_credUP.user}]")
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
      logDebug(s"Listing files of directory[$directory]")

      files = useClient[Array[org.apache.commons.net.ftp.FTPFile]](() => {
        client.changeWorkingDirectory(directory)

        client.listFiles(".")
      })

      logDebug(s"Listed files of directory[$directory]: [${files.mkString(",")}]")

      files.filter(_.isFile).map(x =>
        FTPFile(x.getName, Option(x.getTimestamp.getTime))
      ).toSeq
    })
    match {
      case Success(v) => v
      case Failure(e) => {
        logError(s"Error listing files of directory[$directory]", e);
        throw e
      }
    }
  }

  def download(file: FTPFile, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading file[$file] of directory[$directory]")

      val in = useClient[InputStream](() => {
        client.changeWorkingDirectory(directory)

        client.retrieveFileStream(file.file)
      })

      IOUtils.copy(in, out)

      logDebug(s"Downloaded file[$file] of directory[$directory]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        logError(s"Error downloading file[$file] of directory[$directory]", e);
        throw e
      }
    }
  }
}

object FTPSGdcDownloader {

}
