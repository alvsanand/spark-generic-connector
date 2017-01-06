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

package es.alvsanand.gdc.core.downloader

import java.io.OutputStream

import com.wix.accord._
import es.alvsanand.gdc.core.util.Logging


trait GdcDownloaderParameters extends Serializable

object GdcDownloaderParameters {
  private case class GdcDownloaderParametersImpl() extends GdcDownloaderParameters

  def apply(): GdcDownloaderParameters = new GdcDownloaderParametersImpl
}

/**
  * Created by alvsanand on 3/10/16.
  */
abstract class GdcDownloader[A <: GdcFile, B <: GdcDownloaderParameters]
(protected val parameters: B) extends Logging {

  checkParameters()

  def list(): Seq[A]

  def download(file: A, out: OutputStream): Unit

  protected def getValidator(): Validator[B]

  private def checkParameters(): Unit = {
    implicit val personValidator = getValidator()

    val result: com.wix.accord.Result = validate(parameters)

    if (result.isFailure) {
      throw new IllegalArgumentException(result.toString)
    }
  }
}


trait GdcDownloaderFactory[A <: GdcFile, B <: GdcDownloaderParameters] extends Serializable {
  def get(gdcDownloaderParameters: B): GdcDownloader[A, B]
}
