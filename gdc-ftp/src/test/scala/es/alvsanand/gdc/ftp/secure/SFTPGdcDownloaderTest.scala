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

import java.io.{ByteArrayOutputStream, File}
import java.nio.file.{FileSystems, Files, Paths}
import java.security.{KeyFactory, PublicKey}
import java.security.spec.X509EncodedKeySpec
import java.util.Date

import es.alvsanand.gdc.core.downloader.GdcDownloaderException
import es.alvsanand.gdc.ftp.{FTPCredentials, FTPSlot}
import org.apache.sshd.common.NamedFactory
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.scp.ScpCommandFactory
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.shell.ProcessShellFactory
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory
import org.apache.sshd.server.{Command, SshServer}
import org.scalatest._

import scala.collection.JavaConverters._

class SFTPGdcDownloaderTest extends FlatSpec with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll {

  private val HOST = "127.0.0.1"
  private val PORT = 12022
  private val TEST_USER = "test"
  private val TEST_PASSWORD = "test"
  private val ANON_USER = "anonymous"
  private var server: SshServer = null

  private val TEST_PRIVATE_KEY_FILE = "/servers/config/ftpserver_rsa"
  private val TEST_PRIVATE_KEY_ENCRIPTED_FILE = "/servers/config/ftpserver_rsa_encrypted"
  private val TEST_PRIVATE_KEY_ENCRIPTED_FILE_PASSWORD = "password"
  private val TEST_PUBLIC_KEY_FILE = "/servers/config/ftpserver_rsa.pub"
  private val TEST_JKS_FILE = "/servers/config/ftpserver.jks"
  private val TEST_EMPTY_JKS_FILE = "/servers/config/empty.jks"
  private val TEST_JKS_PASSWORD = "password"
  private val TEST_CONFIG_FILE = "/servers/config/org.apache.ftpserver_users.properties"
  private val TEST_HOME_DIR = "/servers/files"
  private val TEST_SAMPLES_DIR = "samples"
  private val TEST_EMPTY_DIR = "empty"

  override def beforeAll(): Unit = {
    val tmpDir = System.getProperty("java.io.tmpdir")

    server = SshServer.setUpDefaultServer()
    server.setPort(PORT)
    server.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(new File(tmpDir + "/key.ser")))

    server.setFileSystemFactory(new VirtualFileSystemFactory(
      FileSystems.getDefault().getPath(getClass.getResource(TEST_HOME_DIR).getFile)))
    server.setCommandFactory(new ScpCommandFactory())
    server.setShellFactory(new ProcessShellFactory("/bin/sh", "-i", "-l"))
    server.setPasswordAuthenticator(new PasswordAuthenticator() {
      override def authenticate(username: String, password: String, session: ServerSession):
      Boolean = {
        TEST_USER.equals(username) && TEST_PASSWORD.equals(password)
      }
    })

    val keyBytes = Files.readAllBytes(
      new File(getClass.getResource(TEST_PUBLIC_KEY_FILE).getFile).toPath())
    val spec = new X509EncodedKeySpec(keyBytes)
    val kf = KeyFactory.getInstance("RSA");
    val validPublicKey = kf.generatePublic(spec)

    server.setPublickeyAuthenticator(new PublickeyAuthenticator(){
      override def authenticate(username: String, key: PublicKey, session: ServerSession):
      Boolean = {
        TEST_USER.equals(username) && validPublicKey.equals(key)
      }
    })

    server.setSubsystemFactories(List[NamedFactory[Command]](new SftpSubsystemFactory()).asJava)

    server.start()
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  it should "fail with obligatory parameters" in {
    a[IllegalArgumentException] shouldBe
      thrownBy(new SFTPGdcDownloader(SFTPParameters(null, 22, null, null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new SFTPGdcDownloader(SFTPParameters("host", 22, null, null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new SFTPGdcDownloader(SFTPParameters("host", 22, "dir", null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new SFTPGdcDownloader(SFTPParameters("host", 22, "dir", FTPCredentials(null))))
    a[IllegalArgumentException] shouldBe
      thrownBy(new SFTPGdcDownloader(SFTPParameters("host", 22, "dir", FTPCredentials(null),
        Option(KeyConfig(null, null)))))
    a[IllegalArgumentException] shouldBe
      thrownBy(new SFTPGdcDownloader(SFTPParameters("host", 22, "dir", FTPCredentials(null),
        Option(KeyConfig("PKEY", null)))))
  }

  it should "work with obligatory parameters" in {
    noException should be thrownBy (
      new SFTPGdcDownloader(SFTPParameters("host", 22, "dir", FTPCredentials("user"),
        Option(KeyConfig("PKEY", "PKEY"))))
      )
  }

  it should "work with test user and empty/not existing directory" in {
    val parameters = SFTPParameters(HOST, PORT, TEST_EMPTY_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val downloader = new SFTPGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]())
  }

  it should "fail because invalid user" in {
    val parameters = SFTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(ANON_USER))
    val downloader = new SFTPGdcDownloader(parameters)

    a[es.alvsanand.gdc.core.downloader.GdcDownloaderException] shouldBe
      thrownBy(downloader.list())
  }

  it should "work with test user and existing directory" in {
    val parameters = SFTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
    val downloader = new SFTPGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "work with private key" in {
    val parameters = SFTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER),
      pconfig = Option(KeyConfig(getClass.getResource(TEST_PRIVATE_KEY_FILE).getFile,
        getClass.getResource(TEST_PUBLIC_KEY_FILE).getFile)))
    val downloader = new SFTPGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

  it should "work with encrypted private key" in {
    val parameters = SFTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
      FTPCredentials(TEST_USER),
      pconfig = Option(KeyConfig(getClass.getResource(TEST_PRIVATE_KEY_ENCRIPTED_FILE).getFile,
        getClass.getResource(TEST_PUBLIC_KEY_FILE).getFile,
        Option(TEST_PRIVATE_KEY_ENCRIPTED_FILE_PASSWORD))))
    val downloader = new SFTPGdcDownloader(parameters)

    downloader.list().map(_.name) should be(List[String]("sampleFile.txt", "sampleFile2.txt"))
  }

    it should "work with existing name" in {
      val parameters = SFTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
        FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
      val downloader = new SFTPGdcDownloader(parameters)

      val fileName = s"sampleFile.txt"
      val file = s"$TEST_HOME_DIR/$TEST_SAMPLES_DIR/$fileName"

      val data = Files.readAllBytes(Paths.get(getClass.getResource(file).getFile))

      val out: ByteArrayOutputStream = new ByteArrayOutputStream

      downloader.download(FTPSlot(fileName, new Date), out)

      out.toByteArray should be(data)
    }

    it should "fail with bad name" in {
      val parameters = SFTPParameters(HOST, PORT, TEST_SAMPLES_DIR,
        FTPCredentials(TEST_USER, Option(TEST_PASSWORD)))
      val downloader = new SFTPGdcDownloader(parameters)

      val fileName = s"badSampleFile.txt"
      val out: ByteArrayOutputStream = new ByteArrayOutputStream

      a[GdcDownloaderException] should be thrownBy(downloader.download(FTPSlot(fileName, new Date),
        out))
    }
}
