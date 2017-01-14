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

package es.alvsanand.gdc.core.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.zip.{ZipEntry, ZipFile, ZipInputStream}

import es.alvsanand.gdc.core.util.test.HDFSEmbed
import org.scalatest._

import scala.collection.JavaConversions._

class IOUtilsTest extends FlatSpec with Matchers with OptionValues
  with Inside with Inspectors with BeforeAndAfterAll with HDFSEmbed {

  override def beforeAll(): Unit = {
    this.hdfsStart()
  }

  override def afterAll(): Unit = {
    this.hdfsStop()
  }

  it should "Check parseInputStream Normal File" in {
    val file = "/files/example.txt"

    val in = getClass.getResourceAsStream(file)
    val out = IOUtils.parseInputStream(in)

    out should not be null

    org.apache.commons.io.IOUtils.readLines(out)
      .toList should be(List("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))

    in.close()
    out.close()
  }

  it should "Check parseInputStream GZNormal File" in {
    val file = "/files/example.txt.gz"

    val in = getClass.getResourceAsStream(file)
    val out = IOUtils.parseInputStream(in)

    out should not be null

    org.apache.commons.io.IOUtils.readLines(out)
      .toList should be(List("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))

    in.close()
    out.close()
  }

  it should "Check copy" in {
    val s = "FOO"

    val in = new ByteArrayInputStream(s.getBytes)
    val out = new ByteArrayOutputStream()

    IOUtils.copy(in, out)

    new String(out.toByteArray) should be(s)

    in.close()
    out.close()
  }

  it should "Check createTempDirectory" in {
    val dir = IOUtils.createTempDirectory()

    dir should not be null
    dir.isDirectory() should be(true)

    dir.delete()
  }

  it should "Check deleteDirectory" in {
    val dir = IOUtils.createTempDirectory()

    IOUtils.deleteDirectory(dir.getPath)

    dir.exists() should be(false)
  }

  it should "Check getFileType" in {
    IOUtils.getFileType("/FOO.txt") should be(IOUtils.LOCAL_FILE)
    IOUtils.getFileType("name:///FOO.txt") should be(IOUtils.LOCAL_FILE)
    IOUtils.getFileType(s"hdfs://127.0.0.1:$hfdsPort/hdfs_files/example.txt") should
      be(IOUtils.HDFS_FILE)
    a[java.io.IOException] should be thrownBy(IOUtils.getFileType("aaa:///FOO.txt"))
  }

  it should "Check getInputStream for HDFS" in {
    withHDFSFiles(List[(String, String)](("/files/example.txt", "/hdfs_files/example.txt"))) {
      Unit => {
        val in = IOUtils.getInputStream(s"hdfs://127.0.0.1:$hfdsPort/hdfs_files/example.txt")

        in should not be null

        org.apache.commons.io.IOUtils.readLines(in)
          .toList should be(List("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))

        in.close()
      }
    }
  }

  it should "Check unzipToFileDirectory" in {
    val dir = IOUtils.createTempDirectory()

    IOUtils.unzipToFileDirectory(getClass.getResource("/files/files.zip").getFile, dir.getPath)

    dir.list().sorted should be(Array("example.txt", "example_2.txt").sorted)

    IOUtils.deleteDirectory(dir.getPath)
  }

  it should "Check zip" in {
    val dir = IOUtils.createTempDirectory()
    val file = new File(dir.getPath, "test.zip")

    IOUtils.zip(getClass.getResource("/files").getFile, file.getPath)

    val zipFile = new ZipFile(file)

    zipFile.entries() should not be null
    zipFile.entries().toArray.map(_.getName).sorted should
      be(Array("/", "example.txt", "example.txt.gz", "files.zip").sorted)

    IOUtils.deleteDirectory(dir.getPath)
  }

  it should "Check zip to HDFS" in {
    val file = s"hdfs://127.0.0.1:$hfdsPort/all_files.zip"

    IOUtils.zip(getClass.getResource("/files").getFile, file)

    val zipFile = new ZipInputStream(IOUtils.getInputStream(file))

    val entries = scala.collection.mutable.ListBuffer[ZipEntry]()

    Stream.continually(zipFile.getNextEntry).takeWhile(_ != null)
      .foreach { file => entries += file}

    entries should not be null
    entries.toArray.map(_.getName).sorted should
      be(Array("/", "example.txt", "example.txt.gz", "files.zip").sorted)

    zipFile.close()
  }
}
