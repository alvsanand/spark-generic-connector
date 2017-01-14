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

/**
  * A trait that represents the parameters of a es.alvsanand.gdc.core.downloader.GdcDownloader.
  * All parameters must be serializable.
  */
trait GdcDownloaderParameters extends Serializable

object GdcDownloaderParameters {
  private case class GdcDownloaderParametersImpl() extends GdcDownloaderParameters

  def apply(): GdcDownloaderParameters = new GdcDownloaderParametersImpl
}

/**
  * Abstract class that represents a GdcDownloader. A GdcDownloader is designed to connect with a
  * specific system and has the responsibility of creating its own connectors and resources. Due to
  * this GDC is very flexible and ease the task of creating new system integration.
  *
  * A es.alvsanand.gdc.core.downloader.GdcSlot is the smallest part than a GdcDownloader can
  * fetch.
  *
  * Generic Downloader Connector is only able to do reading operations. So basically a
  * GdcDownloader must implement two basic operations:
  *
  *  - def list(): list all the available slots to download.
  *
  *  - def download(slot: A, out: OutputStream): download a specific slot.
  *
  * Finally, a GdcDownloader has two type parameters:
  *
  *  - es.alvsanand.gdc.core.downloader.GdcSlot: a class which contains the type of the slot.
  *
  *  - es.alvsanand.gdc.core.downloader.GdcDownloaderParameters: a class contains the type of
  * parameters.
  *
  * @param parameters The parameters of the GdcDownloader
  * @tparam A The type of es.alvsanand.gdc.core.downloader.GdcSlot can download
  * @tparam B The type of es.alvsanand.gdc.core.downloader.GdcDownloaderParameters that the
  *           receives as parameters.
  */
abstract class GdcDownloader[A <: GdcSlot, B <: GdcDownloaderParameters]
(protected val parameters: B) extends Logging {

  // First operation of the downloader is to check its parameters
  checkParameters()

  /**
    * List all the downloadable slots.
    * @return
    */
  def list(): Seq[A]

  /**
    * Download a name and copy its data content an OutputStream.
    * @param slot The name to download
    * @param out The OutputStream in which the data will be stored.
    */
  def download(slot: A, out: OutputStream): Unit

  /**
    * Abstract method that return the parameters validator
    *
    * @return The parameters validator
    */
  protected def getValidator(): Validator[B]

  /**
    * Check the parameters of the GdcDownloader
    */
  private def checkParameters(): Unit = {
    implicit val personValidator = getValidator()

    val result: com.wix.accord.Result = validate(parameters)

    if (result.isFailure) {
      throw new IllegalArgumentException(result.toString)
    }
  }
}

/**
  * A trait that represents a factory of es.alvsanand.gdc.core.downloader.GdcDownloader.
  * Due to the distributed nature of Spark, a GdcDownloader cannot be shared between the executors.
  * Instead of that, GDC core classes receive these factories and creates new instances of
  * the es.alvsanand.gdc.core.downloader.GdcDownloader.
  * @tparam A Class which contains the metadata information of a name.
  * @tparam B Class which contains the parameters.
  */
trait GdcDownloaderFactory[A <: GdcSlot, B <: GdcDownloaderParameters] extends Serializable {
  /**
    * Method that returns a new instance of a GdcDownloader
    * @param parameters The parameters of the GdcDownloader
    * @return A new instance of a GdcDownloader.
    */
  def get(parameters: B): GdcDownloader[A, B]
}
