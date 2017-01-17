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

/**
  * Generic exception of the SGC library. It always related by a abnormal situation in the
  * es.alvsanand.sgc.core.connector.SgcConnector
  * @param message The message of the exception.
  * @param cause The cause of the exception if exists.
  */
class SgcConnectorException private(message: String = null, cause: Throwable = null)
  extends Exception(message, cause)

/**
  * Simple the task creating new SgcConnectorException
  */
object SgcConnectorException {
  /**
    * Creates a SgcConnectorException
    * @param message The message of the exception.
    * @param cause The cause of the exception if exists.
    * @return A new SgcConnectorException instance
    */
  def apply(message: String = null, cause: Throwable = null): SgcConnectorException = {
    new SgcConnectorException(message, cause)
  }
}
