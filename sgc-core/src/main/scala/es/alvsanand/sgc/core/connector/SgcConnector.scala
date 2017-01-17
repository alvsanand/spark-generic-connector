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

package es.alvsanand.sgc.core.connector

import java.io.OutputStream

import com.wix.accord._
import es.alvsanand.sgc.core.util.Logging

/**
  * A trait that represents the parameters of a es.alvsanand.sgc.core.connector.SgcConnector.
  * All parameters must be serializable.
  */
trait SgcConnectorParameters extends Serializable

object SgcConnectorParameters {
  private case class SgcConnectorParametersImpl() extends SgcConnectorParameters

  def apply(): SgcConnectorParameters = new SgcConnectorParametersImpl
}

/**
  * Abstract class that represents a SgcConnector. A SgcConnector is designed to connect with a
  * specific system and has the responsibility of creating its own clients and resources. Due to
  * this SGC is very flexible and eases the task of integrating with new systems.
  *
  * A es.alvsanand.sgc.core.connector.SgcSlot is the smallest part than a SgcConnector can
  * fetch.
  *
  * Generic Connector is only able to do reading operations. So basically a
  * SgcConnector must implement two basic operations:
  *
  *  - def list(): list all the available slots to fetch.
  *
  *  - def fetch(slot: A, out: OutputStream): fetch a specific slot.
  *
  * Finally, a SgcConnector has two type parameters:
  *
  *  - es.alvsanand.sgc.core.connector.SgcSlot: a class which contains the type of the slot.
  *
  *  - es.alvsanand.sgc.core.connector.SgcConnectorParameters: a class contains the type of
  * parameters.
  *
  * @param parameters The parameters of the SgcConnector
  * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot can fetch
  * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters that the
  *           receives as parameters.
  */
abstract class SgcConnector[A <: SgcSlot, B <: SgcConnectorParameters]
(protected val parameters: B) extends Logging {

  // First operation of the connector is to check its parameters
  checkParameters()

  /**
    * List all the fetchable slots.
    * @return
    */
  @throws(classOf[SgcConnectorException])
  def list(): Seq[A]

  /**
    * Fetch a slot and copy its data content an OutputStream.
    * @param slot The name to fetch
    * @param out The OutputStream in which the data will be stored.
    */
  @throws(classOf[SgcConnectorException])
  def fetch(slot: A, out: OutputStream): Unit

  /**
    * Abstract method that return the parameters validator
    *
    * @return The parameters validator
    */
  protected def getValidator(): Validator[B]

  /**
    * Check the parameters of the SgcConnector
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
  * A trait that represents a factory of es.alvsanand.sgc.core.connector.SgcConnector.
  * Due to the distributed nature of Spark, a SgcConnector cannot be shared between the executors.
  * Instead of that, SGC core classes receive these factories and creates new instances of
  * the es.alvsanand.sgc.core.connector.SgcConnector.
  * @tparam A Class which contains the metadata information of a slot.
  * @tparam B Class which contains the parameters.
  */
trait SgcConnectorFactory[A <: SgcSlot, B <: SgcConnectorParameters] extends Serializable {
  /**
    * Method that returns a new instance of a SgcConnector
    * @param parameters The parameters of the SgcConnector
    * @return A new instance of a SgcConnector.
    */
  def get(parameters: B): SgcConnector[A, B]
}
