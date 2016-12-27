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

package org.apache.spark.util.gdc.downloaders.dcm_data_transfer

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.google.api.client.util.DateTime
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.{Objects, StorageObject}
import org.apache.spark.util.gdc.ReflectionUtils._
import org.apache.spark.util.gdc.downloaders.cloud_storage.CloudStorageFile
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest._

import scala.collection.JavaConversions._

class CloudStorageDownloaderTest extends FlatSpec with Matchers with OptionValues with Inside with Inspectors with BeforeAndAfterAll {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private def createDownloaderList(files: List[(String, Date)]): CloudStorageDownloader = {
    val downloader: CloudStorageDownloader = new CloudStorageDownloader("log4j_test.xml", "bucket");

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
                  val so = new StorageObject();
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

    downloader.setV("_builder", mockBuilder)

    downloader
  }

  it should "fail with empty parameters" in {
    a[IllegalArgumentException] shouldBe thrownBy(new CloudStorageDownloader("FOO", null))
    a[IllegalArgumentException] shouldBe thrownBy(new CloudStorageDownloader("FOO", ""))
    a[IllegalArgumentException] shouldBe thrownBy(new CloudStorageDownloader(null, "FOO"))
    a[IllegalArgumentException] shouldBe thrownBy(new CloudStorageDownloader("", "FOO"))
  }

  it should "work with empty list" in {
    val downloader = createDownloaderList(List[(String, Date)]())

    downloader.list() should be(List[String]())
  }

  private def createDownloaderFile(file: String): CloudStorageDownloader = {
    val downloader: CloudStorageDownloader = new CloudStorageDownloader("log4j_test.xml", "bucket");

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

    downloader.setV("_builder", mockBuilder)

    downloader
  }


  it should "work with simple list" in {
    val downloader = createDownloaderList(List[(String, Date)](("FILE_A.txt", sdf.parse("2019-12-27 00:00:00")),
      ("FILE_B.txt", sdf.parse("2019-12-27 01:00:00")),
      ("FILE_C.txt", sdf.parse("2019-12-27 02:00:00"))))

    downloader.list() should be(List[CloudStorageFile](
      CloudStorageFile("FILE_A.txt", Option(sdf.parse("2019-12-27 00:00:00"))),
      CloudStorageFile("FILE_B.txt", Option(sdf.parse("2019-12-27 01:00:00"))),
      CloudStorageFile("FILE_C.txt", Option(sdf.parse("2019-12-27 02:00:00")))
    ))
  }

  it should "work with existing file" in {
    val downloader = createDownloaderFile("/data_tranfer_files/sampleFile.txt")

    val data = Files.readAllBytes(Paths.get(getClass.getResource("/data_tranfer_files/sampleFile.txt").getFile))

    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    downloader.downloadFile(CloudStorageFile("log4j_test.xml"), out)

    out.toByteArray should be(data)
  }

  it should "fail with bad file" in {
    val downloader = createDownloaderFile(null)

    val out: ByteArrayOutputStream = new ByteArrayOutputStream

    intercept[Throwable] {
      downloader.downloadFile(CloudStorageFile("log4j_test.xml"), out)
    }
  }
}
