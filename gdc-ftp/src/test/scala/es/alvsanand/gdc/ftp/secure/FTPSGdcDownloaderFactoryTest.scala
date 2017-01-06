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

import es.alvsanand.gdc.ftp.Credentials
import org.scalatest._

class FTPSGdcDownloaderFactoryTest extends FlatSpec with Matchers with OptionValues
  with Inside with  Inspectors with BeforeAndAfterAll {

  it should "fail with obligatory parameters" in {
    a[IllegalArgumentException] shouldBe thrownBy(FTPSGdcDownloaderFactory
      .get(FTPSParameters(null, 21, null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSGdcDownloaderFactory
      .get(FTPSParameters("host", 21, null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSGdcDownloaderFactory
      .get(FTPSParameters("host", 21, "dir", null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSGdcDownloaderFactory
      .get(FTPSParameters("host", 21, "dir", Credentials(null))))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSGdcDownloaderFactory
      .get(FTPSParameters("host", 21, "dir", Credentials(null), Option(KeystoreConfig(null)))))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSGdcDownloaderFactory
      .get(FTPSParameters("host", 21, "dir", Credentials(null), Option(KeystoreConfig(null)),
        Option(KeystoreConfig(null)))))
  }

  it should "work with obligatory parameters" in {
    noException should be thrownBy(
      FTPSGdcDownloaderFactory.get(FTPSParameters("host", 21, "dir", Credentials("user"),
        Option(KeystoreConfig("kstore"))))
      )
    noException should be thrownBy(
      FTPSGdcDownloaderFactory.get(FTPSParameters("host", 21, "dir", Credentials("user"),
        Option(KeystoreConfig("kstore")), Option(KeystoreConfig("tstore"))))
      )
  }
}
