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

package es.alvsanand.sgc.core.util

import java.io.{FileOutputStream, _}
import java.net.URI
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.{GZIPInputStream, ZipEntry, ZipInputStream, ZipOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.util.matching.Regex

/**
  * A helper class which perform several System Input/Output operations.
  */
object IOUtils {

  private val GZIP_MAGIC_LENGTH = 2

  /**
    * Detect the type of data that the InputStream contains and wraps it in order to make the data
    * reable.
    * @param inputStream The InputStream with the data.
    * @return The new InputStream.
    */
  def parseInputStream(inputStream: InputStream): InputStream = {
    try {
      inputStream.mark(GZIP_MAGIC_LENGTH);

      val bytes = Array.ofDim[Byte](GZIP_MAGIC_LENGTH)

      inputStream.read(bytes)

      inputStream.reset()
      if (bytes(0) == GZIPInputStream.GZIP_MAGIC.toByte && bytes(1) == (GZIPInputStream
        .GZIP_MAGIC >> 8).toByte) {
        new GZIPInputStream(inputStream)
      }
      else {
        inputStream
      }
    } catch {
      case e: Throwable => inputStream
    }
  }

  /**
    * Copy the data from InputStream to another OutputStream.
    * @param in The source of the data.
    * @param out The destination of the data.
    */
  def copy(in: InputStream, out: OutputStream): Unit = {
    org.apache.commons.io.IOUtils.copy(in, out)
  }

  /**
    * Creates a temporal directory
    * @return The temporal directory name
    */
  def createTempDirectory(): File = {
    val temp = File.createTempFile("temp", System.nanoTime().toString)

    if (!(temp.delete())) {
      throw new IOException("Could not delete temp name: " + temp.getAbsolutePath())
    }

    if (!(temp.mkdir())) {
      throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
    }

    temp
  }

  /**
    * It completely deletes a directory with its files and sub-folders
    * @param directory The directory to delete.
    */
  def deleteDirectory(directory: String): Unit = {
    deleteDirectory(new File(directory))
  }

  /**
    * It completely deletes a directory with its files and sub-folders.
    * @param directory The directory to delete.
    */
  def deleteDirectory(directory: File): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(directory)
  }

  /**
    * A trait that represents the name type of an URL.
    * @param regex The regular expression used to detect the name type of an URL.
    */
  private[util]
  sealed abstract class FILE_TYPE(val regex: Regex)

  /**
    * A case class that represents a HDFS name.
    */
  private[util]
  case object HDFS_FILE extends FILE_TYPE("^hdfs://([\\.:\\w]+)([-_/\\.\\w\\*]+)$".r)

  /**
    * A case class that represents a local name.
    */
  private[util]
  case object LOCAL_FILE extends FILE_TYPE("^name://([-_/\\.\\w\\*]+)$".r)

  /**
    * A case class that represents an unsupported name.
    */
  private[util]
  case object UNSUPPORTED_FILE extends FILE_TYPE("(\\w+)://(.*)".r)

  /**
    * Obtains the type of the name represented in an URL
    * @param url the url of the name.
    * @return The name type.
    */
  private[util]
  def getFileType(url: String): FILE_TYPE = {
    url match {
      case HDFS_FILE.regex(server, path) => HDFS_FILE
      case LOCAL_FILE.regex(path) => LOCAL_FILE
      case UNSUPPORTED_FILE.regex(protocol, path) => throw new IOException(s"$url is not supported")
      case _ => LOCAL_FILE
    }
  }

  /**
    * Return a InputStream from an URL. Currently It supports HDFS and local files.
    * @param url The url of the name.
    * @return The InputStream which reads the data
    */
  def getInputStream(url: String): InputStream = {
    getFileType(url) match {
      case HDFS_FILE => {
        val HDFS_FILE.regex(server, filePath) = url

        val fs = FileSystem.get(new URI(s"hdfs://$server"), new Configuration())
        val path = new org.apache.hadoop.fs.Path(filePath)

        if (!fs.exists(path)) {
          throw new IOException(s"$url does not exist")
        }

        fs.open(path)
      }
      case LOCAL_FILE => {
        val filePath = url match {
          case LOCAL_FILE.regex(path) => path
          case _ => url
        }

        val file = new File(filePath)

        if (!file.exists()) {
          throw new IOException(s"$url does not exist")
        }

        new FileInputStream(file)
      }
      case _ => throw new IOException(s"$url is not supported")
    }
  }

  /**
    * Return a OutputStream from an URL. Currently It supports HDFS and local files.
    * @param url The url of the name.
    * @return The OutputStream which write the data
    */
  def getOutputStream(url: String): OutputStream = {
    getFileType(url) match {
      case HDFS_FILE => {
        val HDFS_FILE.regex(server, filePath) = url

        val fs = FileSystem.get(new URI(s"hdfs://$server"), new Configuration())
        val path = new org.apache.hadoop.fs.Path(filePath)

        if (path.getParent().getParent != null && !fs.exists(path.getParent())) {
          throw new IOException(s"$url cannot be created because parent path does not exist")
        }

        fs.create(path)
      }
      case LOCAL_FILE => {
        val filePath = url match {
          case LOCAL_FILE.regex(path) => path
          case _ => url
        }

        val file = new File(filePath)

        if (file.getParentFile() != null && !file.getParentFile().exists()) {
          throw new IOException(s"$url cannot be created because parent folder does not exist")
        }

        if (!file.exists()) {
          file.createNewFile()
        }

        new FileOutputStream(file)
      }
      case _ => throw new IOException(s"$url is not supported")
    }
  }

  /**
    * Return the data from an URL. Currently It supports HDFS and local files.
    * @param url The url of the name.
    * @return The data bytes of the name.
    */
  def getBytes(url: String): Array[Byte] = {
    val out = new ByteArrayOutputStream()

    copy(getInputStream(url), out)

    out.toByteArray
  }

  /**
    * Unzip an url into a local Folder.
    * @param urlZip The url of the zip.
    * @param outputDir The destination directory.
    */
  def unzipToFileDirectory(urlZip: String, outputDir: String): Unit = {
    var in: ZipInputStream = null
    val outputDirPath = Paths.get(outputDir)

    if (!Files.exists(outputDirPath)) {
      throw new IOException(s"$outputDir does not exist")
    }

    try {
      in = new ZipInputStream(getInputStream(urlZip))

      Stream.continually(in.getNextEntry).takeWhile(_ != null)
        .foreach { file =>
          if (!file.getName.equals("/")) {
            val fileOut = new FileOutputStream(outputDirPath.resolve(file.getName).toFile)
            IOUtils.copy(in, fileOut)

            fileOut.close()
          }
        }
    }
    finally {
      if (in != null) {
        in.close()
      }
    }
  }

  /**
    * Zip a local Folder.
    * @param dir The local directory to be zipped
    * @param outputZip The url of the destination zip name.
    */
  def zip(dir: String, outputZip: String): Unit = {
    val dirPath = Paths.get(dir)
    val outputZipPath = Paths.get(outputZip)

    if (!Files.exists(dirPath)) {
      throw new IOException(s"$dir does not exist")
    }

    Files.deleteIfExists(outputZipPath)

    val fileOutputZip = new ZipOutputStream(getOutputStream(outputZip))

    try {
      Files.walkFileTree(dirPath, new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, basicFileAttributes: BasicFileAttributes):
        FileVisitResult
        = {
          fileOutputZip.putNextEntry(new ZipEntry(dirPath.relativize(file).toString()))
          Files.copy(file, fileOutputZip)
          fileOutputZip.closeEntry()

          FileVisitResult.CONTINUE
        }

        override def preVisitDirectory(dir: Path, basicFileAttributes: BasicFileAttributes):
        FileVisitResult = {
          fileOutputZip.putNextEntry(new ZipEntry(dirPath.relativize(dir).toString() + "/"))
          fileOutputZip.closeEntry()

          FileVisitResult.CONTINUE
        }
      })
    }
    finally {
      if (fileOutputZip != null) {
        fileOutputZip.close()
      }
    }
  }
}
