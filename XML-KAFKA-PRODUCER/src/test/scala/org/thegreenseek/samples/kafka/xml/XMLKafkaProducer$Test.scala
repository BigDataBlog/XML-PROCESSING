package org.thegreenseek.samples.kafka.xml

import java.util.Properties

import org.thegreenseek.samples.kafka.fwk
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.thegreenseek.samples.kafka.fwk.{HadoopClient, KafkaClient, KafkaFramework, KafkaUtilities}

import scala.collection.mutable.ArrayBuffer
import java.io.{BufferedReader, InputStream, InputStreamReader}

import scala.collection.JavaConverters._

/**
  * Created by Macphil11 on 19/07/2016.
  */
class XMLKafkaProducer$Test extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  override def afterEach() {

  }

  /**
    * Test parseXmlAndSendMessage
    */
  test("parseXmlAndSendMessage") {

    val topic: String = "landsat"

    // This will be delegated the print of messages to stdout
    def printMessage (ctopic:String, cmessage:String): Int = {
        println("BUFFER PRINT START")
        println(cmessage)
        println("BUFFER PRINT END")
        return 0
    }

    XMLKafkaProducer.parseXmlAndSendMessage(
      HadoopClient.instance().getInpuStream("/user/Macphil1/landsat-small.xml"), printMessage, topic
    )

  }

  test("testLoadDefaultProducerProperties") {

    var defProps = KafkaUtilities.loadDefaultProducerProperties
    assert(defProps.isDefined)

    if(defProps.isDefined) {
      defProps.foreach(props => props.asScala.toMap.foreach(item => println(item._1) + " : " + println(item._2)))
    }

  }

  test("parseXmlAndSendMessage" + "KafkaClient.sendStringRecord") {
    val topic: String = "landsat"

    XMLKafkaProducer.parseXmlAndSendMessage(
      HadoopClient.instance().getInpuStream("/user/Macphil1/landsat-small.xml"),
      KafkaClient.instance().sendStringRecord, topic)

  }

  /*test("readAutocommit") {
    val topic: String = "landsat"
    val fwk = new KafkaFramework()

    def printMessage (offset:Long, key:String, value:Any): Int = {
      println("BUFFER PRINT START READING MESSAGES")
      println("OFFSET: %l" + offset + " - KEY: %s" + key + " - MESSAGE:")
      println(value)
      println("BUFFER PRINT END READING MESSAGES")
      return 0
    }

    fwk.readAutocommit(topic,printMessage)
  }*/

}
