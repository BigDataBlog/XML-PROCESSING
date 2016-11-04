package org.thegreenseek.samples.kafka.fwk

import java.util.Properties

import org.apache.kafka.clients.producer._

/**
  * Created by Macphil1 on 29/10/2016.
  */
class KafkaStringProducer (properties: Properties) {
  var dproducer = new KafkaProducer[String, String](properties)

  /**
    * this message sender function will be used by file parsers
    * to feed kafka topics
    *
    * @param lproducer
    * @param message
    * @param topic
    * @return
    */
  def sendRecord(lproducer: Any, message: String, topic: String): Int = {

    /*Option(lproducer) match {
      case None => lproducer = dproducer
    }
    producer = */

      return 0
  }
}
