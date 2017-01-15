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

package es.alvsanand.sgdc.google.cloud_storage

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.google.api.client.util.DateTime
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.{Objects, StorageObject}
import es.alvsanand.sgdc.core.util.ReflectionUtils._
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest._

import scala.collection.JavaConversions._

class CloudStorageSgdcDownloaderTest extends FlatSpec with Matchers with OptionValues with Inside
  with Inspectors with
  BeforeAndAfterAll {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private def createDownloaderList(files: List[(String, Date)]): CloudStorageSgdcDownloader = {
    val downloader = new CloudStorageSgdcDownloader(CloudStorageParameters("FOO", "FOO"))

    val mockBuilder: Storage = Mockito.mock(classOf[Storage])

    Mockito.doAnswer(new Answer[Any]() {
      def answer(invocation: InvocationOnMock): mockBuilder.Objects = {
        val arg1: Array[AnyRef] = invocation.getArguments

        val mockObjects = Mockito.mock(classOf[mockBuilder.Objects])

        Mockito.doAnswer(new Answer[Any]() {
          def answer(invocation: InvocationOnMock): mockObjects.List = {
            val arg2: Array[AnyRef] = invocation.getArguments

            val mockList = Mockito.mock(classOf[mockObjects.List])

            Mockito.doAnswer(new Answer[Any]() {
              def answer(invocation: InvocationOnMock): Objects = {
                val arg3: Array[AnyRef] = invocation.getArguments

                val objects = new com.google.api.services.storage.model.Objects

                objects.setItems(files.map(f => {
                  val so = new StorageObject()
                  so.setName(f._1);
                  so.setTimeCreated(new DateTime(f._2, TimeZone.getTimeZone("GMT")))
                  so
                }))
              }
            }).when(mockList).execute()

            mockList
          }
        }).when(mockObjects).list(org.mockito.Matchers.anyString())

        mockObjects
      }
    }).when(mockBuilder).objects()

    downloader.setV("_client", mockBuilder)

    downloader
  }

  private def createDownloaderFile(file: String): CloudStorageSgdcDownloader = {
    val downloader = new CloudStorageSgdcDownloader(CloudStorageParameters("FOO", "FOO"))

    val mockBuilder: Storage = Mockito.mock(classOf[Storage])

    Mockito.doAnswer(new Answer[Any]() {
      def answer(invocation: InvocationOnMock): mockBuilder.Objects = {
        val arg1: Array[AnyRef] = invocation.getArguments

        val mockObjects = Mockito.mock(classOf[mockBuilder.Objects])

        Mockito.doAnswer(new Answer[Any]() {
          def answer(invocation: InvocationOnMock): mockObjects.Get = {
            val arg2: Array[AnyRef] = invocation.getArguments

            val mockGet = Mockito.mock(classOf[mockObjects.Get])

            Mockito.doAnswer(new Answer[Any]() {
              def answer(invocation: InvocationOnMock): Any = {
                val arg3: Array[AnyRef] = invocation.getArguments

                if (file == null) {
                  throw new Exception()
                }

                val out = arg3(0).asInstanceOf[java.io.OutputStream]

                val in = getClass.getResourceAsStream(file)
                out.write(
                  Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
                )
                out.close()
              }
            }).when(mockGet).executeMediaAndDownloadTo(org.mockito.Matchers.any[OutputStream]())

            mockGet
          }
        }).when(mockObjects).get(org.mockito.Matchers.anyString(), org.mockito.Matchers.anyString())

        mockObjects
      }
    }).when(mockBuilder).objects()

    downloader.setV("_client", mockBuilder)

    downloader
  }

  it should "fail with empty parameters" in {
    a[IllegalArgumentException] shouldBe
      thrownBy(new CloudStorageSgdcDownloader(CloudStorageParameters("FOO", null)))
    a[IllegalArgumentException] shouldBe
      thrownBy(new CloudStorageSgdcDownloader(CloudStorageParameters("FOO", "")))
    a[IllegalArgumentException] shouldBe
      thrownBy(new CloudStorageSgdcDownloader(CloudStorageParameters(null, "FOO")))
    a[IllegalArgumentException] shouldBe
      thrownBy(new CloudStorageSgdcDownloader(CloudStorageParameters("", "FOO")))
  }

  it should "work with empty list" in {
    val downloader = createDownloaderList(List[(String, Date)]())

    downloader.list() should be(List[String]())
  }

  it should "work with simple list" in {
    val downloader = createDownloaderList(List[(String, Date)](("FILE_A.txt", sdf.parse
    ("2019-12-27 00:00:00")),
      ("FILE_B.txt", sdf.parse("2019-12-27 01:00:00")),
      ("FILE_C.txt", sdf.parse("2019-12-27 02:00:00"))))

    downloader.list() should be(List[CloudStorageSlot](
      CloudStorageSlot("FILE_A.txt", sdf.parse("2019-12-27 00:00:00")),
      CloudStorageSlot("FILE_B.txt", sdf.parse("2019-12-27 01:00:00")),
      CloudStorageSlot("FILE_C.txt", sdf.parse("2019-12-27 02:00:00"))
    ))
  }

  it should "work with existing name" in {
    val downloader = createDownloaderFile("/sample_files/sampleFile.txt")

    val data = Files.readAllBytes(Paths.get(getClass.getResource("/sample_files/sampleFile" +
      ".txt").getFile))

    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    downloader.download(CloudStorageSlot("log4j_test.xml", null), out)

    out.toByteArray should be(data)
  }

  it should "fail with bad name" in {
    val downloader = createDownloaderFile(null)

    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    intercept[Throwable] {
      downloader.download(CloudStorageSlot("log4j_test.xml", null), out)
    }
  }
}
