package org.thegreenseek.samples.kafka.fwk

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by Macphil11 on 12/08/2016.
  */
class KafkaMessagesHandler(props: Properties) {
  private var stringProducer: Producer[String, String] = null
  private var lProps: Properties = props

  def sendStringMessage(topic: String, message: String): Int = {
    stringProducer = new KafkaProducer[String, String](lProps)
    try {
      stringProducer.send(new ProducerRecord[String, String](topic, message))
    }
    catch {
      case ex: Exception => {
        return -1
      }
    } finally {
      stringProducer.close
    }
    return 0
  }
}
