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

package es.alvsanand.sgc.ftp

import java.util.Date

import es.alvsanand.sgc.core.connector.SgcDateSlot

/**
  * The es.alvsanand.sgc.core.connector.SgcDateSlot implementation for
  * FTP server like es.alvsanand.sgc.core.connector.SgcConnector.
  *
  * Note: every FTPSlot corresponds to a file in the FTP server like
  *
  * @param name The name of the file
  * @param date The last modification time of the file
  */
case class FTPSlot(name: String, date: Date) extends SgcDateSlot
