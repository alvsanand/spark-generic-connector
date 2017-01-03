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

package es.alvsanand.gdc.ftp.normal

import java.io._

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.ftp.{Credentials, FTPFile, HasUserPassword}
import org.apache.commons.net.ftp.{FTPClient, FTPHTTPClient, FTPReply}

import scala.util.{Failure, Success, Try}


/**
  * Created by alvsanand on 30/09/16.
  */

/**
  * FTP implementation od¡f GdcDownloader.
  *
  * @param host The host of the FTP server
  * @param _cred The port of the FTP server
  * @param directory The directory path where the dowloader will find files
  * @param clientConfigMap FTPClient specific parameters:
  *  <ul>
  *    <li><i>proxyEnabled</i>: HTTP proxy is enabled</li>
  *    <li><i>proxyHost</i>: HTTP proxy host</li>
  *    <li><i>proxyPort</i>: HTTP proxy port</li>
  *    <li><i>proxyUser</i>: HTTP proxy user name</li>
  *    <li><i>proxyPassword</i>: HTTP proxy password</li>
  *    <li>
  *      <i>defaultTimeout</i>: the default timeout to use (in ms). Default: 120 seconds.
  *    </li>
  *    <li>
  *      <i>dataTimeout</i>: the timeout used of the data connection (in ms). Default: 1200 seconds.
  *    </li>
  *  </ul>
  */
class FTPGdcDownloader(host: String, port: Int, cred: Credentials,
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
    throw new IllegalArgumentException("credentials must be of type HasUserPassword")
  }

  val _cred = cred.asInstanceOf[HasUserPassword]

  if (directory == null || directory == "") {
    throw new IllegalArgumentException("directory cannot be empty")
  }

  if (clientConfigMap.getOrElse("proxyEnabled", "false").toBoolean
      && !clientConfigMap.contains("proxyHost")) {
    throw new IllegalArgumentException("proxyHost cannot be empty if proxyEnabled is true")
  }

  if (clientConfigMap.getOrElse("proxyEnabled", "false").toBoolean
    && !clientConfigMap.getOrElse("proxyPort", "").forall(_.isDigit)) {
    throw new IllegalArgumentException("proxyPort must be number")
  }

  private val client: FTPClient = initClient()

  private def initClient(): FTPClient = synchronized {
    var client: FTPClient = null

    logInfo(s"Initiating FTPDownloader[host: $host, user: ${_cred.user}]")

    clientConfigMap.get("proxyEnabled") match {
      case Some(proxyEnabled) if proxyEnabled.toBoolean => {
        val proxyHost = clientConfigMap.getOrElse("proxyHost", "")
        val proxyPort = clientConfigMap.getOrElse("proxyPort", "80").toInt
        val proxyUser = clientConfigMap.getOrElse("proxyUser", "")
        val proxyPassword = clientConfigMap.getOrElse("proxyPassword", "")

        client = new FTPHTTPClient(proxyHost, proxyPort, proxyUser, proxyPassword)
      }
      case _ => client = new FTPClient()
    }

    client.enterLocalPassiveMode()

    var defaultTimeout = DEFAULT_DATA_TIMEOUT
    var dataTimeout = DEFAULT_TIMEOUT

    clientConfigMap.foreach{ case (k, v) => k match {
        case "defaultTimeout" if v.forall(_.isDigit) => defaultTimeout = v.toInt
        case "dataTimeout" if v.forall(_.isDigit) => dataTimeout = v.toInt
        case _ =>
      }
    }
    client.setDefaultTimeout(defaultTimeout)
    client.setDataTimeout(dataTimeout)

    logInfo(s"Initiated FTPDownloader[host: $host, user: ${_cred.user}]")

    client
  }

  def usesProxy(): Boolean = client.isInstanceOf[FTPHTTPClient]

  private def connect(): Unit = {
    if (!client.isConnected) {
      logInfo(s"Connecting FTPDownloader[host: $host, user: ${_cred.user}]")

      client.connect(host, port);

      val reply = client.getReplyCode();

      if (!FTPReply.isPositiveCompletion(reply)) {
        throw GdcDownloaderException(s"Error connecting to host[$host]: $reply");
      }
      if (!client.login(_cred.user, _cred.password)) {
        throw GdcDownloaderException(s"Error logging in to host[$host] and user[${_cred.user}]");
      }

      logInfo(s"Connecting FTPDownloader[host: $host, user: ${_cred.user}]")
    }
  }

  private def disconnect(): Unit = {
    logInfo(s"Disconnecting FTPDownloader[host: $host, user: ${_cred.user}]")

    if (client.isConnected) {
      client.disconnect()
    }

    logInfo(s"Disconnecting FTPDownloader[host: $host, user: ${_cred.user}]")
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

object FTPGdcDownloader {

}
