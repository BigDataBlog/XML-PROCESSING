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

    val buf = ArrayBuffer[String]()
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
    def sendMessage (prod: Any, cbuf: ArrayBuffer[String], ctopic:String) : Int = {
      prod.asInstanceOf[KafkaStringProducer].sendRecord(ctopic, cbuf.toString())
    }
    // End Local Send Function

    val xmlFile = args(0)
    parseXmlFile(xmlFile, sendMessage, producer, buf, topic )


  }

  /**
    * Dedicated to Landsat model parsing
    * Parse XML file as events
    * @param xmlFile
    */
   def parseXmlFile (xmlFile: String, send: (Any, ArrayBuffer[String], String) => Int, cproducer: Any, cbuf: ArrayBuffer[String], ctopic: String): Unit = {

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
           send(cproducer, cbuf, ctopic)
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
