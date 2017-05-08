package org.thegreenseek.samples.kafka.xml


import java.io.InputStream

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
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


  /**
    * Dedicated to Landsat model parsing
    * Parse XML file as events and
    * sends message using the provided send method:
    * it optimizes resources usage
    * TODO : test if the send method exist
    * @param xmlFile input stream targeting the xml file to parse
    * @param send A signature of the method that will send the record to Kafka (topic,message)
    * @param ctopic the Kafka Topic to send the messag to
    */
   def parseXmlAndSendMessage (xmlFile: InputStream, send: (String, String) => Int, ctopic: String): Unit = {

     val cbuf = ArrayBuffer[String]()
     // TODO : manage exception for XML file not found
     val xml = new XMLEventReader(Source.fromInputStream(xmlFile))

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
            send(ctopic, cbuf.toString())
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
