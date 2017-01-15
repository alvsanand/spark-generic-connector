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

package es.alvsanand.sgdc.ftp.secure

import es.alvsanand.sgdc.core.downloader.SgdcDownloaderException
import es.alvsanand.sgdc.ftp.FTPCredentials
import org.apache.commons.io.FileUtils
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.ssl.SslConfigurationFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.{FtpServer, FtpServerFactory}
import org.scalatest._

class FTPSSgdcDownloaderClientAuthTest extends FlatSpec with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll {

  private val HOST = "127.0.0.1"
  private val PORT = 12421
  private val TEST_USER = "test"
  private val TEST_PASSWORD = "test"
  private val ANON_USER = "anonymous"
  private val ANON_PASSWORD = ""
  private var server: FtpServer = null

  private val TEST_JKS_FILE = "/servers/config/ftpserver.jks"
  private val TEST_EMPTY_JKS_FILE = "/servers/config/empty.jks"
  private val TEST_JKS_PASSWORD = "password"
  private val TEST_JKS_TYPE = "JKS"
  private val TEST_CONFIG_FILE = "/servers/config/org.apache.ftpserver_users.properties"
  private val TEST_HOME_DIR = "/servers/files"
  private val TEST_SAMPLES_DIR = "samples"
  private val TEST_EMPTY_DIR = "empty"

  override def beforeAll(): Unit = {
    val propertiesUserManagerFactory = new PropertiesUserManagerFactory()
    val users = getClass.getResource(TEST_CONFIG_FILE)
    propertiesUserManagerFactory.setUrl(users)

    val userManager = propertiesUserManagerFactory.createUserManager()

    val testUser = userManager.getUserByName(TEST_USER).asInstanceOf[BaseUser]
    testUser.setHomeDirectory(getClass.getResource(TEST_HOME_DIR).getFile)
    userManager.save(testUser)
    val anonymousUser = userManager.getUserByName(ANON_USER).asInstanceOf[BaseUser]
    anonymousUser.setHomeDirectory(getClass.getResource(TEST_HOME_DIR).getFile)
    userManager.save(anonymousUser)

    val serverFactory = new FtpServerFactory()
    serverFactory.setUserManager(userManager)
    val factory = new ListenerFactory()
    factory.setPort(PORT)

    val serverJksResource = getClass.getResource(TEST_JKS_FILE)
    val ssl = new SslConfigurationFactory()
    val keyStoreFile = FileUtils.toFile(serverJksResource)
    ssl.setKeystoreFile(keyStoreFile)
    ssl.setKeystorePassword(TEST_JKS_PASSWORD)
    ssl.setClientAuthentication("true")

    factory.setSslConfiguration(ssl.createSslConfiguration())
    factory.setImplicitSsl(false)

    serverFactory.addListener("default", factory.createListener())

    server = serverFactory.createServer()
    server.start()
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  it should "work with client auth" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)),
      kconfig = Option(KeystoreConfig(getClass.getResource(TEST_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))),
      tconfig = Option(KeystoreConfig(getClass.getResource(TEST_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))))
    val downloader = new FTPSSgdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "fail because empty keystore" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)),
      kconfig = Option(KeystoreConfig(getClass.getResource(TEST_EMPTY_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))),
      tconfig = Option(KeystoreConfig(getClass.getResource(TEST_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))))
    val downloader = new FTPSSgdcDownloader(parameters)

    a[SgdcDownloaderException] shouldBe thrownBy(downloader.list().map(_.name))
  }

  it should "fail because need keystore" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)),
      tconfig = Option(KeystoreConfig(getClass.getResource(TEST_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))))
    val downloader = new FTPSSgdcDownloader(parameters)

    a[SgdcDownloaderException] shouldBe thrownBy(downloader.list().map(_.name))
  }
}
