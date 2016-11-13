package org.thegreenseek.samples.kafka.xml


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, Producer}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.pull._
//import scala.xml.XML


/**
  * Created by Macphil11 on 14/07/2016.
  */
object XMLKafkaProducer {

  val props: Properties = new Properties()

  var inMetadata = false

  //var metaDataTag = "metadata"

  def main(args: Array[String]) {


    val topic: String = "landsat"

    // Debug
    args.map(p => println("Print XML File : " + p))
    // End Debug


    // required properties and initialization of Kafka producer
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaStringProducer = new KafkaStringProducer(props)
    // End producer initialization

    // Local Send function --> will use a special producer to send the buffer
    def sendMessage (prod: Any, ctopic:String, cmessage: String) : Int = {
      prod.asInstanceOf[KafkaStringProducer].sendRecord(ctopic, cmessage)
    }
    // End Local Send Function

    val xmlFile = args(0)
    parseXmlAndSendMessage(xmlFile, sendMessage, producer, topic )


  }

  /**
    * Dedicated to Landsat model parsing
    * Parse XML file as events and
    * sends message using the provided send method:
    * it optimizes resources usage
    * TODO : test if the send method exist
    * @param xmlFile
    * @param send A signature of the method that will send the record to Kafka
    * @param producer A potential producer to send the record
    * @param ctopic the Kafka Topic to send the messag to
    */
   def parseXmlAndSendMessage (xmlFile: String, send: (Any, String, String) => Int, producer: Any, ctopic: String): Unit = {

     val cbuf = ArrayBuffer[String]()
     // TODO : manage exception for XML file not found
     val xml = new XMLEventReader(Source.fromFile(xmlFile))

     for (event <- xml) {
       event match {
         case EvElemStart(_, "metaData", _, _) => {
           inMetadata = true
           val tag = "<metaData>"
           cbuf += tag
         }
         case EvElemEnd(_, "metaData") => {
           val tag = "</metaData>"
           cbuf += tag
           inMetadata = false

           // send message
           if(Some(send).isDefined)
            send(producer, ctopic, cbuf.toString())
           // end send message
           cbuf.clear
         }
         case e @ EvElemStart(_, tag, _, _) => {
           if (inMetadata) {
             cbuf += ("<" + tag + ">")
           }
         }
         case e @ EvElemEnd(_, tag) => {
           if (inMetadata) {
             cbuf += ("</" + tag + ">")
           }
         }
         case EvText(t) => {
           if (inMetadata) {
             cbuf += (t)
           }
         }
         case _ => // ignore
       }
     }
   }
}
