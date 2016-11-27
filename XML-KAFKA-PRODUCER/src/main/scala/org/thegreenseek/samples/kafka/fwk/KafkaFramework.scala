package org.thegreenseek.samples.kafka.fwk

import java.util.Properties

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
    * @param topicName
    * @param handle this method witll handle the consumed messages
    */
  def readAutocommit(topicName: String, handle:(Int,String, Any ) => Int): Unit = {

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
