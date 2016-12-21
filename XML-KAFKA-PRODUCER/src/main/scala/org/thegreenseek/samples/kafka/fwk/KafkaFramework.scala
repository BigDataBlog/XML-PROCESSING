package org.thegreenseek.samples.kafka.fwk

//scala
//import scala.language.implicitConversions
import scala.collection.JavaConversions._ //converts java collections to scala collections

import util.control.Breaks._
//java
import java.util
import java.util.Properties
//Kafka
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, ConsumerRecord}
import org.apache.kafka.clients.producer._

/**
  * Created by Macphil1 on 29/10/2016.d of property update is useless
  */
class KafkaFramework {

  /**
    * this message sender function will be used by file parsers
    * to feed kafka topics
    * Version without key
    * TODO : investigate the usage of keys in ProducerRecords
    * @param topic
    * @param message
    * @param sPropertiesSend tries to setup special producer properties
    * @return information telling if anything went wrong
    */
  def sendStringRecord( topic: String, message: String, sPropertiesSend: String): Int = {

    var lProperties = new Properties()

    if(null == sPropertiesSend)
      lProperties = KafkaUtilities.loadDefaultProducerProperties.head //Use default producer
    else
      lProperties = KafkaUtilities.loadProperties(sPropertiesSend).head


    if(null !=lProperties && null != topic && null != message) {
      val record = new ProducerRecord[String, String](topic, System.currentTimeMillis().toString, message)
      //val record = new ProducerRecord[String, String](topic, message)
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](lProperties)
      producer.send(record)

      return 0
    }
    return -1
  }

  /**
    * Consume messages from the specified topic with autocommit true.
    * It uses the default consumer properties
    * @param topicName
    * @param handleCommit this method will handle the consumed messages
    */
  def readAutocommit(topicName: String, handleCommit:(Long,String, Any ) => Int): Unit = {

    val lProperties = KafkaUtilities.loadDefaultConsumerProperties.head


    if(null !=lProperties && null != topicName ) {
        val consumer = new KafkaConsumer[String,String](lProperties)
        consumer.subscribe(util.Arrays.asList(topicName))
        breakable {
          while (true) {
            try {
              val consumerR: ConsumerRecords[String, String] = consumer.poll(Constants.KAFKAConsumerTimeout)
              consumerR.foreach(record => handleCommit(record.offset, record.key, record.value))
            } catch {
              case allDone: Exception => break //TODO Update exception management
            }
          }
        }
    }

  }

  /**
    * create a new KafkaTopic
    * TODO : to move in an admin framework far from end users hands
    * @param topicName
    * @return
    */
  def createTopic(topicName: String): Int = {
    return -1
  }

  /**
    * Check if topocName exists then delete it
    * TODO : to move in an admin framework far from end users hands
    * @param topicName
    * @return
    */
  def deleteTopic(topicName: String): Int = {
    return -1
  }
}
