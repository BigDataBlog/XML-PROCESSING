package org.thegreenseek.samples.kafka.fwk

import java.util.Properties

import org.apache.kafka.clients.producer._

/**
  * Created by Macphil1 on 29/10/2016.
  */
class KafkaStringProducer {
  //var dproducer = new KafkaProducer[String, String](properties)
  var dProperties = KafkaUtilities.loadDefaultProperties

  /**
    * this message sender function will be used by file parsers
    * to feed kafka topics
    *
    * @param specProperties
    * @param topic
    * @param key
    * @param message
    * @return
    */
  def sendRecord(specProperties: Any, topic: String, key: String , message: String): Int = {

    var lProperties = new Properties()

    if(!Some(specProperties).isDefined) //If producer with special properties does not exist
      lProperties = dProperties.head //Use default producer

    if(Some(topic).isDefined && Some(key).isDefined && Some(message).isDefined ) {
      val record = new ProducerRecord(topic, key, message)
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](lProperties)
      producer.send(record)

      return 0
    }
    return -1
  }
}
