package org.thegreenseek.samples.kafka.fwk

import java.util.Properties

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by Macphil11 on 05/05/2017.
  */
class KafkaClient private(val properties: String){

  val LOGGER: Logger = LoggerFactory.getLogger(KafkaClient.getClass.getName)

  //Setup Properties
  var lProperties = new Properties()

  if(null == properties)
    lProperties = KafkaUtilities.loadDefaultProducerProperties.head //Use default producer
  else
    lProperties = KafkaUtilities.loadProperties(properties).head

  //instanciate producer
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](lProperties)

  //handle shutdownhook
  sys.ShutdownHookThread {
    producer.close()
  }

  def sendStringRecord( topic: String, message: String): Int = {
    LOGGER.info("KAFKA_CLIENT.sendStringRecord : START")
    if(null != topic && null != message) {
      val record = new ProducerRecord[String, String](topic, System.currentTimeMillis().toString, message)

      producer.send(record)
      LOGGER.info("KAFKA_CLIENT.sendStringRecord : MESSAGE SENT")
      return 0
    }
    LOGGER.info("KAFKA_CLIENT.sendStringRecord : MESSAGE NOT SENT")
    return -1
  }
}

/**
  *
  */
object KafkaClient {

  val _instance: KafkaClient = new KafkaClient(null)

  def instance():KafkaClient = {
    _instance
  }
}
