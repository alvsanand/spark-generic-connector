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
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException, GdcDownloaderParameters}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.ftp._
import org.apache.commons.net.ftp.{FTPClient, FTPReply, FTPSClient}
import com.wix.accord.Validator
import com.wix.accord.dsl.{be, _}

import scala.util.{Failure, Success, Try}

/**
  * The parameters for es.alvsanand.gdc.ftp.secure.FTPSGdcDownloader.
  *
  * @param host The host of the FTPS server [Required].
  * @param port The port of the FTPS server. Default: 990.
  * @param directory The directory path where the downloader will find files [Required].
  * @param cred The credentials used for logging into the FTPS server [Required].
  * @param kconfig The keystore used to log in the FTPS server. See
  *                [[https://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html]] for
  *                more details.
  * @param tconfig The truststore used to log in the FTPS server. See
  *                [[https://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html]] for
  *                more details.
  * @param defaultTimeout the default timeout to use (in ms). Default: 120 seconds.
  * @param dataTimeout The timeout used of the data connection (in ms). Default: 1200 seconds.
  */
case class FTPSParameters(host: String, port: Int = 990, directory: String,
                          cred: FTPCredentials, kconfig: Option[KeystoreConfig] = None,
                          tconfig: Option[KeystoreConfig] = None,
                          defaultTimeout: Int = 120000, dataTimeout: Int = 1200000)
  extends GdcDownloaderParameters {
}

/**
  * This is [[https://en.wikipedia.org/wiki/File_Transfer_Protocol FTPS server]] implementation of
  * es.alvsanand.gdc.core.downloader.GdcDownloader. It list and download all the files that are in
  * a configured directory.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The FTP client will authenticate using the credentials.
  *
  *  - If the keystore is set, the FTPS client set NeedClientAuth to true. That means the client must
  *  use a a certificate to create the SSL connection and the server must validate the client
  *  certificate.
  *
  *  - If truststore is set, the FTPS client will check that the server certificate
  *  is valid using that truststore. That means that if the sever certificate is not in the
  *  truststore the connection will fail.
  *
  * @param parameters The parameters of the GdcDownloader
  */
private[secure]
class FTPSGdcDownloader(parameters: FTPSParameters)
  extends GdcDownloader[FTPSlot, FTPSParameters](parameters) {

  /** @inheritdoc */
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
      if(p.kconfig.isDefined) p.kconfig.get.url is notNull
      if(p.kconfig.isDefined) p.kconfig.get.url is notEmpty
      if(p.kconfig.isDefined) p.kconfig.get.keystoreType is notNull
      if(p.kconfig.isDefined) p.kconfig.get.keystoreType is notEmpty
      if(p.tconfig.isDefined) p.tconfig.get.url is notNull
      if(p.tconfig.isDefined) p.tconfig.get.url is notEmpty
      if(p.tconfig.isDefined) p.tconfig.get.keystoreType is notNull
      if(p.tconfig.isDefined) p.tconfig.get.keystoreType is notEmpty
      p.defaultTimeout should be > 0
      p.dataTimeout should be > 0
    }
  }

  private lazy val client: FTPClient = initClient()

  /**
    * Method that initialize the FTPS client.
    * @return The FTPS client
    */
  private def initClient(): FTPClient = synchronized {
    logInfo(s"Initiating FTPDownloader[$parameters]")

    val client: FTPSClient = new FTPSClient()

    client.enterLocalPassiveMode()

    client.setDefaultTimeout(parameters.defaultTimeout)
    client.setDataTimeout(parameters.dataTimeout)

    if(parameters.kconfig.isDefined){
      client.setNeedClientAuth(true)

      val kconfig = parameters.kconfig.get
      val ks = KeyStore.getInstance(kconfig.keystoreType)
      ks.load(IOUtils.getInputStream(kconfig.url),
        kconfig.keystorePassword.getOrElse("").toCharArray)

      val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(ks, kconfig.keystorePassword.getOrElse("").toCharArray)

      val keyManagers = keyManagerFactory.getKeyManagers()
      client.setKeyManager(keyManagers(0))
    }

    if(parameters.tconfig.isDefined){
      val tconfig = parameters.tconfig.get
      val ts = KeyStore.getInstance(tconfig.keystoreType)
      ts.load(IOUtils.getInputStream(tconfig.url),
        tconfig.keystorePassword.getOrElse("").toCharArray)

      val keyManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(ts)

      val trustManagers = keyManagerFactory.getTrustManagers()
      client.setTrustManager(trustManagers(0))
    }

    logInfo(s"Initiated FTPDownloader[$parameters]")

    client
  }

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

      logInfo(s"Connecting FTPDownloader[$parameters, user: ${parameters.cred.user}]")
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
