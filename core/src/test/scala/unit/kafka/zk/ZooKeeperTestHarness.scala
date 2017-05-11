/**
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

package kafka.zk

import javax.security.auth.login.Configuration

import kafka.utils.{CoreUtils, Logging, ZkUtils}
import org.junit.{After, Before}
import org.junit.Assert.assertEquals
import org.scalatest.junit.JUnitSuite
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category

import scala.collection.JavaConverters._

@Category(Array(classOf[IntegrationTest]))
abstract class ZooKeeperTestHarness extends JUnitSuite with Logging {

  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 6000
  protected val zkAclsEnabled: Option[Boolean] = None

  var zkUtils: ZkUtils = null
  var zookeeper: EmbeddedZookeeper = null

  def zkPort: Int = zookeeper.port
  def zkConnect: String = s"127.0.0.1:$zkPort"
  
  @Before
  def setUp() {
    assertNoBrokerControllersRunning()
    zookeeper = new EmbeddedZookeeper()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled()))
  }

  @After
  def tearDown() {
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close())
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown())
    Configuration.setConfiguration(null)
    assertNoBrokerControllersRunning()
  }

  // Tests using this class start ZooKeeper before starting any brokers and shutdown ZK after
  // shutting down brokers. If tests leave broker controllers running, subsequent tests may fail in
  // unexpected ways if ZK port is reused. This method ensures that there is no Controller event thread
  // since the controller loads default JAAS configuration to make connections to brokers on this thread.
  //
  // Any tests that use this class and invoke ZooKeeperTestHarness#tearDown() will fail in the tearDown()
  // if controller event thread is found. Tests with missing broker shutdown which don't use ZooKeeperTestHarness
  // or its tearDown() will cause an assertion failure in the subsequent test that invokes ZooKeeperTestHarness#setUp(),
  // making it easier to identify the test with missing shutdown from the test sequence.
  private def assertNoBrokerControllersRunning() {
    val threads = Thread.getAllStackTraces.keySet.asScala
      .map(_.getName)
      .filter(_.contains("controller-event-thread"))
    assertEquals(Set(), threads)
  }
}
