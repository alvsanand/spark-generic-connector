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

import com.wix.accord.Validator
import com.wix.accord.dsl.{be, _}
import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException, GdcDownloaderParameters}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.ftp.{FTPCredentials, FTPSlot}
import org.apache.commons.net.ftp.{FTPClient, FTPHTTPClient, FTPReply}

import scala.util.{Failure, Success, Try}



/**
  * The parameters for es.alvsanand.gdc.ftp.normal.FTPGdcDownloader.
  *
  * @param host The host of the FTP server [Obligatory].
  * @param port The port of the FTP server. Default: 21.
  * @param cred The credentials used for logging into the FTP server [Obligatory].
  * @param directory The directory path where the downloader will find files [Obligatory].
  * @param defaultTimeout the default timeout to use (in ms). Default: 120 seconds.
  * @param dataTimeout The timeout used of the data connection (in ms). Default: 1200 seconds.
  */
case class FTPParameters(host: String, port: Int = 21, directory: String,
                         cred: FTPCredentials, defaultTimeout: Int = 120000,
                         dataTimeout: Int = 1200000, proxyEnabled: Boolean = false,
                         proxyHost: Option[String] = None, proxyPort: Int = 0,
                         proxyUser: Option[String] = None, proxyPassword: Option[String] = None)
  extends GdcDownloaderParameters {
  override def toString: String =
    s"FTPSParameters($host, $port, $cred, $directory, $defaultTimeout, $dataTimeout, " +
      s"$proxyEnabled, $proxyHost, $proxyPort, $proxyUser, ***)"
}

/**
  * This is a [[https://en.wikipedia.org/wiki/File_Transfer_Protocol FTP server]] implementation of
  * es.alvsanand.gdc.core.downloader.GdcDownloader. It list and download all the files that are in
  * a configured directory.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The FTP client will authenticate using the credentials.
  *
  *  - If the proxy parameters are set, the FTP client will use a proxy instead of a direct
  * connection.
  *
  * @param parameters The parameters of the GdcDownloader
  */
private[normal]
class FTPGdcDownloader(parameters: FTPParameters)
  extends GdcDownloader[FTPSlot, FTPParameters](parameters) {

  /** @inheritdoc */
  override def getValidator(): Validator[FTPParameters] = {
    validator[FTPParameters] { p =>
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
      if(p.proxyEnabled){
        p.proxyHost is notNull
        p.proxyHost is notEmpty
      }
    }
  }

  private lazy val client: FTPClient = initClient()

  /**
    * Method that initialize the FTP client.
    * @return The FTP client.
    */
  private def initClient(): FTPClient = synchronized {
    var client: FTPClient = null

    logInfo(s"Initiating FTPDownloader[$parameters]")

    client = parameters.proxyEnabled match {
      case true => new FTPHTTPClient(parameters.proxyHost.getOrElse(""), parameters.proxyPort,
          parameters.proxyUser.getOrElse(""), parameters.proxyPassword.getOrElse(""))
      case _ => new FTPClient()
    }

    client.enterLocalPassiveMode()

    client.setDefaultTimeout(parameters.defaultTimeout)
    client.setDataTimeout(parameters.dataTimeout)

    logInfo(s"Initiated FTPDownloader[$parameters]")

    client
  }

  /**
    * Check if the client use a HTTP Proxy.
    * @return True if the client use a HTTP Proxy.
    */
  def usesProxy(): Boolean = client.isInstanceOf[FTPHTTPClient]

  /**
    * Connects to server
    */
  private def connect(): Unit = {
    if (!client.isConnected) {
      logInfo(s"Connecting FTPDownloader[$parameters]")

      Try(client.connect(parameters.host, parameters.port)) match {
        case Failure(e) => throw GdcDownloaderException(s"Error connecting to server", e)
        case _ =>
      }

      val reply = client.getReplyCode();

      if (!FTPReply.isPositiveCompletion(reply)) {
        throw GdcDownloaderException(s"Error connecting to server: $reply")
      }
      if (!client.login(parameters.cred.user, parameters.cred.password.getOrElse(""))) {
        throw GdcDownloaderException(s"Error logging in with user[${parameters.cred.user}]")
      }

      logInfo(s"Connecting FTPDownloader[$parameters]")
    }
  }

  /**
    * Disconnects from server
    */
  private def disconnect(): Unit = {
    logInfo(s"Disconnecting FTPDownloader[$parameters]")

    if (client.isConnected) {
      client.disconnect()
    }

    logInfo(s"Disconnecting FTPDownloader[$parameters]")
  }

  /**
    * Helper method to use the client
    */
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

  /** @inheritdoc */
  override def list(): Seq[FTPSlot] = {
    var files: Array[org.apache.commons.net.ftp.FTPFile] = Array.empty

    Try({
      logDebug(s"Listing files of directory[${parameters.directory}]")

      files = useClient[Array[org.apache.commons.net.ftp.FTPFile]](() => {
        client.changeWorkingDirectory(parameters.directory)

        client.listFiles(".")
      })

      logDebug(s"Listed files of directory[${parameters.directory}]: [${files.mkString(",")}]")

      files.filter(_.isFile).map(x =>
        FTPSlot(x.getName, x.getTimestamp.getTime)
      ).sortBy(_.name).toSeq
    })
    match {
      case Success(v) => v
      case Failure(e) => {
        val msg = s"Error listing files of directory[${parameters.directory}]"
        logError(msg, e)
        throw GdcDownloaderException(msg, e)
      }
    }
  }

  /** @inheritdoc */
  override def download(file: FTPSlot, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading name[$file] of directory[${parameters.directory}]")

      val in = useClient[InputStream](() => {
        client.changeWorkingDirectory(parameters.directory)

        client.retrieveFileStream(file.name)
      })

      if (in != null) {
        IOUtils.copy(in, out)

        in.close()
      }

      logDebug(s"Downloaded name[$file] of directory[${parameters.directory}]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        val msg = s"Error downloading name[$file] of directory[${parameters.directory}]"
        logError(msg, e)
        throw GdcDownloaderException(msg, e)
      }
    }
  }
}
