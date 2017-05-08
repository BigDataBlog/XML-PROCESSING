package org.thegreenseek.samples.kafka.fwk

import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by Macphil11 on 05/05/2017.
  */
class KafkaClientTest extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  override def afterEach() {

  }

  test("testSendStringRecord") {
    KafkaClient.instance().sendStringRecord("test_topic","message")
  }

}
