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

package es.alvsanand.sgc.ftp.secure

import java.io._
import java.util.Date

import com.jcraft.jsch.{ChannelSftp, JSch, KeyPair, Session}
import com.wix.accord.Validator
import com.wix.accord.dsl.{be, _}
import es.alvsanand.sgc.core.connector.{SgcConnector, SgcConnectorException, SgcConnectorParameters}
import es.alvsanand.sgc.core.util.IOUtils
import es.alvsanand.sgc.ftp._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * The parameters for es.alvsanand.sgc.ftp.secure.SFTPSgcConnector.
  *
  * @param host The host of the SFTP server [Obligatory].
  * @param port The port of the SFTP server. Default: 22.
  * @param directory The directory path where the connector will find files [Obligatory].
  * @param cred The credentials used for logging into the SFTP server [Obligatory].
  * @param pconfig The private/public key used to log in the SFTP server.
  * @param timeout the default timeout to use (in ms). Default: 120 seconds.
  */
case class SFTPParameters(host: String, port: Int = 22, directory: String,
                          cred: FTPCredentials, pconfig: Option[KeyConfig] = None,
                          timeout: Int = 120000)
  extends SgcConnectorParameters {
}

/**
  * This is a [[https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol SFTP server]]
  * implementation of es.alvsanand.sgc.core.connector.SgcConnector. It list and fetch all the
  * files that are in
  * a configured directory.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The SFTP client will authenticate using the credentials.
  *  - If the keystore is set, the SFTP client will use the private key to authenticate instead of
  *  the password.
  *
  * @param parameters The parameters of the SgcConnector
  */
private[secure]
class SFTPSgcConnector(parameters: SFTPParameters)
  extends SgcConnector[FTPSlot, SFTPParameters](parameters) {

  /** @inheritdoc */
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

  /**
    * Method that initialize the SFTP client.
    * @return The SFTP client
    */
  private def initClient(): Session = synchronized {
    logInfo(s"Initiating FTPConnector[$parameters]")

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

    logInfo(s"Initiated FTPConnector[$parameters]")

    client
  }

  /**
    * Connects to server
    */
  private def connect(): Unit = {
    if (!client.isConnected) {
      logInfo(s"Connecting FTPConnector[$parameters]")

      Try(client.connect()) match {
        case Failure(e) => throw SgcConnectorException(s"Error connecting to server", e)
        case _ =>
      }

      logInfo(s"Connecting FTPConnector[$parameters, user: ${parameters.cred.user}]")
    }
  }

  /**
    * Disconnects from server
    */
  private def disconnect(): Unit = {
    logInfo(s"Disconnecting FTPConnector[$parameters]")

    if (client.isConnected) {
      client.disconnect()
    }

    logInfo(s"Disconnecting FTPConnector[$parameters]")
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
  @throws(classOf[SgcConnectorException])
  override def list(): Seq[FTPSlot] = {
    Try({
      logDebug(s"Listing files of directory[${parameters.directory}]")

      val files = useClient[Seq[FTPSlot]](() => {
        val channel = client.openChannel("sftp").asInstanceOf[ChannelSftp]
        channel.connect()

        try {
          val files = channel.ls(parameters.directory)
          if (files != null) {
            files.asScala.map(_.asInstanceOf[channel.LsEntry]).toArray
              .filterNot(f => f.getAttrs.isDir || f.getAttrs.isLink)
              .map(f => FTPSlot(s"${f.getFilename}", new Date(f.getAttrs.getMTime * 1000L)))
              .sortBy(_.name)
          }
          else {
            Seq.empty[FTPSlot]
          }
        } catch {
          case e: Exception => {
            logWarning(s"Error listing files of directory[${parameters.directory}]: " +
              s"${e.getMessage}")

            Seq.empty[FTPSlot]
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
        throw SgcConnectorException(msg, e)
      }
    }
  }

  /** @inheritdoc */
  @throws(classOf[SgcConnectorException])
  override def fetch(slot: FTPSlot, out: OutputStream): Unit = {
    Try({
      logDebug(s"Fetching slot[$slot] of directory[${parameters.directory}]")

      useClient[Unit](() => {
        val channel = client.openChannel("sftp").asInstanceOf[ChannelSftp]
        channel.connect()

        try {
          channel.cd(parameters.directory)


          val in = channel.get(slot.name)

          if (in != null) {
            IOUtils.copy(in, out)

            in.close()
          }
        } finally {
          channel.disconnect()
        }
      })

      logDebug(s"Fetched slot[$slot] of directory[${parameters.directory}]")
    })
    match {
      case Success(v) =>
      case Failure(e) => {
        val msg = s"Error fetching slot[$slot] of directory[${parameters.directory}]"
        logError(msg, e)
        throw SgcConnectorException(msg, e)
      }
    }
  }
}
