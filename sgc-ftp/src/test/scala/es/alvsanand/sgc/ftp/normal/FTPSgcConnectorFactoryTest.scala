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

package es.alvsanand.sgc.ftp.normal

import es.alvsanand.sgc.ftp.{FTPCredentials, ProxyConfiguration}
import org.scalatest._

class FTPSgcConnectorFactoryTest extends FlatSpec with Matchers with OptionValues
  with Inside with  Inspectors with BeforeAndAfterAll {

  it should "fail with obligatory parameters" in {
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters(null, 21, null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, "directory", null)))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, "directory", FTPCredentials(null))))
    a[IllegalArgumentException] shouldBe thrownBy(FTPSgcConnectorFactory
      .get(FTPParameters("host", 21, "directory", FTPCredentials("user"), proxy = Option(ProxyConfiguration("")))))
  }

  it should "work with obligatory parameters" in {
    noException should be thrownBy(
      FTPSgcConnectorFactory.get(FTPParameters("host", 21, "directory", FTPCredentials("user")))
      )
    noException should be thrownBy(
      FTPSgcConnectorFactory.get(FTPParameters("host", 21, "directory", FTPCredentials("user"),
          proxy = Option(ProxyConfiguration("proxyHost"))))
      )
  }

  it should "work with proxy parameters" in {
    var p = Option(ProxyConfiguration("proxyHost", user = Option("user")))
    var parameters = FTPParameters("host", 21, "directory", FTPCredentials("user"),
      proxy = p)
    noException should be thrownBy(FTPSgcConnectorFactory.get(parameters))
    FTPSgcConnectorFactory.get(parameters).asInstanceOf[FTPSgcConnector].usesProxy() should
      be(true)

    p = Option(ProxyConfiguration("proxyHost", user = Option("user"), password = Option("")))
    parameters = FTPParameters("host", 21, "directory", FTPCredentials("user"),
      proxy = p)
    noException should be thrownBy(FTPSgcConnectorFactory.get(parameters))
    FTPSgcConnectorFactory.get(parameters).asInstanceOf[FTPSgcConnector].usesProxy() should
      be(true)
  }
}
