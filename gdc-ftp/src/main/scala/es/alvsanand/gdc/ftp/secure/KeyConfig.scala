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

package es.alvsanand.gdc.ftp.secure

/**
  * This class contains the SSH authentication configuration.
  *
  * Note: only is supported pbes2 Encrypted Private Key.
  *
  * @param privateUrl The url to find the Private Key.
  * @param publicUrl The url to find the Public Key.
  * @param privatePassword The password used in the Private Key.
  */
case class KeyConfig(privateUrl: String, publicUrl: String, privatePassword: Option[String] = None){
  override def toString: String = s"KeyConfig($privateUrl,  $publicUrl, ***)"
}
