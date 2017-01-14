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

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Paths}
import java.util.Date

import es.alvsanand.gdc.ftp.{FTPCredentials, FTPSlot}
import org.apache.commons.io.FileUtils
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.ssl.SslConfigurationFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.{FtpServer, FtpServerFactory}
import org.scalatest._

class FTPSGdcDownloaderTest extends FlatSpec with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll {

  private val HOST = "127.0.0.1"
  private val PORT = 12421
  private val TEST_USER = "test"
  private val TEST_PASSWORD = "test"
  private val ANON_USER = "anonymous"
  private var server: FtpServer = null

  private val TEST_JKS_FILE = "/servers/config/ftpserver.jks"
  private val TEST_EMPTY_JKS_FILE = "/servers/config/empty.jks"
  private val TEST_JKS_PASSWORD = "password"
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

    // define SSL configuration
    val serverJksResource = getClass.getResource(TEST_JKS_FILE)
    val ssl = new SslConfigurationFactory()
    val keyStoreFile = FileUtils.toFile(serverJksResource)
    ssl.setKeystoreFile(keyStoreFile)
    ssl.setKeystorePassword(TEST_JKS_PASSWORD)

    // set the SSL configuration for the listener
    factory.setSslConfiguration(ssl.createSslConfiguration())
    factory.setImplicitSsl(false)

    serverFactory.addListener("default", factory.createListener())

    server = serverFactory.createServer()
    server.start()
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  it should "fail with obligatory parameters" in {
    a[IllegalArgumentException] shouldBe
      thrownBy(new FTPSGdcDownloader(FTPSParameters(null, 21, null, null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new FTPSGdcDownloader(FTPSParameters("host", 21, null, null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new FTPSGdcDownloader(FTPSParameters("host", 21, "dir", null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new FTPSGdcDownloader(FTPSParameters("host", 21, "dir", FTPCredentials(null))))
    a[IllegalArgumentException] shouldBe
      thrownBy(new FTPSGdcDownloader(FTPSParameters("host", 21, "dir", FTPCredentials(null),
        Option(KeystoreConfig(null)))))
    a[IllegalArgumentException] shouldBe
      thrownBy(new FTPSGdcDownloader(FTPSParameters("host", 21, "dir", FTPCredentials(null),
        Option(KeystoreConfig(null)), Option(KeystoreConfig(null)))))
  }

  it should "work with obligatory parameters" in {
    noException should be thrownBy(
      new FTPSGdcDownloader(FTPSParameters("host", 21, "dir", FTPCredentials("user"),
        Option(KeystoreConfig("kstore"))))
      )
    noException should be thrownBy(
      new FTPSGdcDownloader(FTPSParameters("host", 21, "dir", FTPCredentials("user"),
        Option(KeystoreConfig("kstore")), Option(KeystoreConfig("tstore"))))
      )
  }

  it should "work with test user and empty/not existing directory" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_EMPTY_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val downloader = new FTPSGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]())
  }

  it should "work with anonymous user and existing directory" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(ANON_USER))
    val downloader = new FTPSGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "work with test user and existing directory" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val downloader = new FTPSGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "work with truststore" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)),
      tconfig = Option(KeystoreConfig(getClass.getResource(TEST_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))))
    val downloader = new FTPSGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "fail because truststore" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)),
      tconfig = Option(KeystoreConfig(getClass.getResource(TEST_EMPTY_JKS_FILE).getFile,
        keystorePassword = Option(TEST_JKS_PASSWORD))))
    val downloader = new FTPSGdcDownloader(parameters)

    a[es.alvsanand.gdc.core.downloader.GdcDownloaderException] shouldBe
      thrownBy(downloader.list().map(_.name))
  }

  it should "work with existing name" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val downloader = new FTPSGdcDownloader(parameters)

    val fileName = s"sampleFile.txt"
    val file = s"$TEST_HOME_DIR/$TEST_SAMPLES_DIR/$fileName"

    val data = Files.readAllBytes(Paths.get(getClass.getResource(file).getFile))

    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    downloader.download(FTPSlot(fileName, new Date), out)

    out.toByteArray should be(data)
  }

  it should "fail with bad name" in {
    val parameters = FTPSParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val downloader = new FTPSGdcDownloader(parameters)

    val fileName = s"badSampleFile.txt"
    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    noException should be thrownBy(downloader.download(FTPSlot(fileName, new Date), out))
    out.size() should be(0)
  }
}
