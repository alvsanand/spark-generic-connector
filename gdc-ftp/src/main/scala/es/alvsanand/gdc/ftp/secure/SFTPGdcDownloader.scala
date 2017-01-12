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
import java.util.Date

import com.jcraft.jsch.{ChannelSftp, JSch, KeyPair, Session}
import com.wix.accord.Validator
import com.wix.accord.dsl.{be, _}
import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderException, GdcDownloaderParameters}
import es.alvsanand.gdc.core.util.IOUtils
import es.alvsanand.gdc.ftp._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * SFTPGdcDownloader parameters.
  *
  * @param host The host of the SFTP server
  * @param port The port of the SFTP server. Default: 22.
  * @param directory The directory path where the downloader will find files
  * @param cred The credentials used for logging into the SFTP server
  * @param kconfig The private/public key used to log in the SFTP server
  * @param timeout the default timeout to use (in ms). Default: 120 seconds.
  */
case class SFTPParameters(host: String, port: Int = 22, directory: String,
                          cred: Credentials, pconfig: Option[KeyConfig] = None,
                          timeout: Int = 120000)
  extends GdcDownloaderParameters {
}

private[secure]
class SFTPGdcDownloader(parameters: SFTPParameters)
  extends GdcDownloader[FTPFile, SFTPParameters](parameters) {

  override def getValidator(): Validator[SFTPParameters] = {
    validator[SFTPParameters] { p =>
      p.host is notNull
      p.host is notEmpty
      p.port should be > 0
      p.directory is notNull
      p.directory is notEmpty
      p.cred is notNull
      if(p.cred!=null) p.cred.user is notNull
      if(p.cred!=null) p.cred.user is notEmpty
      if(p.pconfig.isDefined) p.pconfig.get.privateUrl is notNull
      if(p.pconfig.isDefined) p.pconfig.get.privateUrl is notEmpty
      if(p.pconfig.isDefined) p.pconfig.get.publicUrl is notNull
      if(p.pconfig.isDefined) p.pconfig.get.publicUrl is notEmpty
      p.timeout should be > 0
    }
  }

  private lazy val client: Session = initClient()

  private def initClient(): Session = synchronized {
    logInfo(s"Initiating FTPDownloader[$parameters]")

    val jsch = new JSch()
    val client = jsch.getSession(new String(parameters.cred.user), parameters.host, parameters.port)

    if(parameters.cred.password.isDefined){
      client.setPassword(parameters.cred.password.get)
    }
    if(parameters.pconfig.isDefined){
      val kp = KeyPair.load(jsch, IOUtils.getBytes(parameters.pconfig.get.privateUrl),
        IOUtils.getBytes(parameters.pconfig.get.publicUrl))

      val t = kp.decrypt(parameters.pconfig.get.privatePassword.getOrElse("").getBytes)

      jsch.addIdentity("credentials", IOUtils.getBytes(parameters.pconfig.get.privateUrl),
        IOUtils.getBytes(parameters.pconfig.get.publicUrl),
        parameters.pconfig.get.privatePassword.getOrElse("").getBytes)
    }

    client.setTimeout(parameters.timeout)

    val config = new java.util.Properties()
    config.put("StrictHostKeyChecking", "no")
    client.setConfig(config)

    logInfo(s"Initiated FTPDownloader[$parameters]")

    client
  }

  private def connect(): Unit = {
    if (!client.isConnected) {
      logInfo(s"Connecting FTPDownloader[$parameters]")

      Try(client.connect()) match {
        case Failure(e) => throw GdcDownloaderException(s"Error connecting to server", e)
        case _ =>
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
    Try({
      logDebug(s"Listing files of directory[${parameters.directory}]")

      val files = useClient[Seq[FTPFile]](() => {
        val channel = client.openChannel("sftp").asInstanceOf[ChannelSftp]
        channel.connect()

        try {
          val files = channel.ls(parameters.directory)
          if (files != null) {
            files.asScala.map(_.asInstanceOf[channel.LsEntry]).toArray
              .filterNot(f => f.getAttrs.isDir || f.getAttrs.isLink)
              .map(f => FTPFile(s"${f.getFilename}", Option(new Date(f.getAttrs.getMTime * 1000L))))
              .sortBy(_.file)
          }
          else {
            Seq.empty[FTPFile]
          }
        } catch {
          case e: Exception => {
            logWarning(s"Error listing files of directory[${parameters.directory}]: " +
              s"${e.getMessage}")

            Seq.empty[FTPFile]
          }
        } finally {
          channel.disconnect()
        }
      })

      logDebug(s"Listed files of directory[${parameters.directory}]: [${files.mkString(",")}]")

      files
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

  def download(file: FTPFile, out: OutputStream): Unit = {
    Try({
      logDebug(s"Downloading file[$file] of directory[${parameters.directory}]")

      useClient[Unit](() => {
        val channel = client.openChannel("sftp").asInstanceOf[ChannelSftp]
        channel.connect()

        try {
          channel.cd(parameters.directory)


          val in = channel.get(file.file)

          if (in != null) {
            IOUtils.copy(in, out)

            in.close()
          }
        } finally {
          channel.disconnect()
        }
      })

      logDebug(s"Downloaded file[$file] of directory[${parameters.directory}]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        val msg = s"Error downloading file[$file] of directory[${parameters.directory}]"
        logError(msg, e)
        throw GdcDownloaderException(msg, e)
      }
    }
  }
}
