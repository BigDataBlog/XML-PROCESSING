package org.thegreenseek.samples.kafka.xml

import java.util.Properties

import org.thegreenseek.samples.kafka.fwk

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.thegreenseek.samples.kafka.fwk.KafkaUtilities
import scala.collection.mutable.ArrayBuffer
import java.io.{InputStreamReader, BufferedReader, InputStream}
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
    * Test testParseXmlFile
    */
  test("testParseXmlFile") {

    val topic: String = "landsat"

    def printMessage (prod: Any, ctopic:String, cmessage:String): Int = {
        println("BUFFER PRINT START")
        println(cmessage)
        println("BUFFER PRINT END")
        return 0
    }

    XMLKafkaProducer.parseXmlAndSendMessage(
      "/Users/Macphil11/Documents/Projets/GitHub/XML-PROCESSING/XML-KAFKA-PRODUCER/data/landsat-small.xml", printMessage, null, topic
    )

  }

  test("testLoadDefaultProperties") {

    var defProps = KafkaUtilities.loadDefaultProperties
    assert(defProps.isDefined)

    if(defProps.isDefined) {
      defProps.foreach(props => props.asScala.toMap.foreach(item => println(item._1) + " : " + println(item._2)))
    }

  }

}
