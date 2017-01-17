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

package es.alvsanand.sgc.google.dcm_data_transfer

import es.alvsanand.sgc.core.connector.{SgcConnector, SgcConnectorFactory}

/**
  * This Factory creates a instances of es.alvsanand.sgc.core.connector.SgcConnector for
  * integrating with
  *  [[https://support.google.com/dcm/partner/answer/165589?hl=en DoubleClick Data Transfer]].
  *  It list and fetch all the
  *  files that are in a configured bucket.
  *
  * Note: every file will be used as a slot.
  *
  * It has these features:
  *
  *  - The CloudStorage client will authenticate using the credentials zip. See
  *                           es.alvsanand.sgc.google.GoogleHelper to know more about the
  *                           credentials zip file.
  *
  *  - The Google authentication method used by this implementation is Authorization code flow using
  * a GooglePromptReceiver. See
  *  [[https://developers.google.com/api-client-library/java/google-api-java-client/oauth2
  *  Authorization Code Flow]] for more info.
  *
  *  - The SgcConnector is able to filter the type of Data Transfer files.
  */
object DataTransferSgcConnectorFactory
    extends SgcConnectorFactory[DataTransferSlot, DataTransferParameters] {

  /**
    * Method that returns a new instance of a SgcConnector
    * @param parameters The parameters of the SgcConnector
    * @return A new instance of a SgcConnector.
    */
  override def get(parameters: DataTransferParameters):
      SgcConnector[DataTransferSlot, DataTransferParameters] = {
    new DataTransferSgcConnector(parameters)
  }
}
