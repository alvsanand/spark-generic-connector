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

package es.alvsanand.sgc.ftp.normal

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Paths}
import java.util.Date

import es.alvsanand.sgc.core.connector.SgcConnectorException
import es.alvsanand.sgc.ftp.{FTPCredentials, FTPSlot, ProxyConfiguration}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.{FtpServer, FtpServerFactory}
import org.scalatest._

class FTPSgcConnectorTest extends FlatSpec with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll {

  private val HOST = "127.0.0.1"
  private val PORT = 12021
  private val TEST_USER = "test"
  private val TEST_PASSWORD = "test"
  private val ANON_USER = "anonymous"
  private var server: FtpServer = null

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
    serverFactory.setUserManager(userManager);
    val factory = new ListenerFactory();
    factory.setPort(PORT);
    serverFactory.addListener("default", factory.createListener());

    server = serverFactory.createServer();
    server.start();
  }

  override def afterAll(): Unit = {
    server.stop();
  }

  it should "fail with obligatory parameters" in {
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters(null, 21, null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, "directory", null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, "directory", FTPCredentials(null))))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, "directory", FTPCredentials("user"),
        proxy = Option(ProxyConfiguration("")))))
  }

  it should "work with obligatory parameters" in {
    noException should be thrownBy(
      new FTPSgcConnector(FTPParameters("host", 21, "directory", FTPCredentials("user")))
      )
    noException should be thrownBy(
      new FTPSgcConnector(FTPParameters("host", 21, "directory", FTPCredentials("user"),
        proxy = Option(ProxyConfiguration("proxyHost"))))
      )
  }

  it should "work with proxy parameters" in {
    var p = Option(ProxyConfiguration("proxyHost", user = Option("user")))
    var parameters = FTPParameters("host", 21, "directory", FTPCredentials("user"),
      proxy = p)
    noException should be thrownBy(new FTPSgcConnector(parameters))
    new FTPSgcConnector(parameters).asInstanceOf[FTPSgcConnector].usesProxy() should
      be(true)

    p = Option(ProxyConfiguration("proxyHost", user = Option("user"), password = Option("")))
    parameters = FTPParameters("host", 21, "directory", FTPCredentials("user"),
      proxy = p)
    noException should be thrownBy(new FTPSgcConnector(parameters))
    new FTPSgcConnector(parameters).asInstanceOf[FTPSgcConnector].usesProxy() should
      be(true)
  }

  it should "work with test user and empty/not existing directory" in {
    val parameters = FTPParameters(HOST, PORT, TEST_EMPTY_DIR,
                                   FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val connector = new FTPSgcConnector(parameters)

    a[SgcConnectorException] shouldBe thrownBy(connector.list())
  }

  it should "work with anonymous user and existing directory" in {
    val parameters = FTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(ANON_USER))
    val connector = new FTPSgcConnector(parameters)

    connector.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "work with test user and existing directory" in {
    val parameters = FTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val connector = new FTPSgcConnector(parameters)

    connector.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "work with existing name" in {
    val parameters = FTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val connector = new FTPSgcConnector(parameters)

    val fileName = s"sampleFile.txt"
    val file = s"$TEST_HOME_DIR/$TEST_SAMPLES_DIR/$fileName"

    val data = Files.readAllBytes(Paths.get(getClass.getResource(file).getFile))

    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    connector.fetch(FTPSlot(fileName, new Date), out)

    out.toByteArray should be(data)
  }

  it should "fail with bad name" in {
    val parameters = FTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val connector = new FTPSgcConnector(parameters)

    val fileName = s"badSampleFile.txt"
    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    noException should be thrownBy(connector.fetch(FTPSlot(fileName, new Date), out))
    out.size() should be(0)
  }
}
