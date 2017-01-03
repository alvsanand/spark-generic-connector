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

import java.io._
import java.util.zip.GZIPInputStream

object IOUtils {

  private val GZIP_MAGIC_LENGTH = 2

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

  def copy(in: InputStream, out: OutputStream): Unit = {
    org.apache.commons.io.IOUtils.copy(in, out)
  }

  @throws(classOf[IOException])
  def createTempDirectory(): File = {
    val temp = File.createTempFile("temp", System.nanoTime().toString)

    if (!(temp.delete())) {
      throw new IOException("Could not delete temp file: " + temp.getAbsolutePath())
    }

    if (!(temp.mkdir())) {
      throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
    }

    temp
  }

  def getInputStream(url: String): InputStream = {
    new FileInputStream(url)
  }
}
