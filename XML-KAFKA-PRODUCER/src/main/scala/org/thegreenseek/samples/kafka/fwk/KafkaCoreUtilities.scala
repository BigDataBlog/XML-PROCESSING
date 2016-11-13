package org.thegreenseek.samples.kafka.fwk

import java.util.Properties
import java.io.{FileInputStream, IOException}

/**
  * Created by Macphil11 on 12/08/2016.
  * updated 30 Ocy 2016: result is now wrapped in Option
  * @return Some or None
  */
object KafkaUtilities {
  def loadDefaultProducerProperties: Option[Properties] = {
    val dprops = new Properties()
    try {
        val inpropsFile = KafkaUtilities.getClass.getResourceAsStream("/DefaultKafkaProducerProperties.xml")
        if(Option(inpropsFile).isDefined)
          dprops.loadFromXML(inpropsFile)
        else
          throw new KafkaLoadPropertiesException("DefaultKafkaProducerProperties.xml not found")
      return Option(dprops)
    } catch {
      case ioex: IOException => {
        println("COULD NOT LOAD DEFAULT KAFKA PRODUCER PROPS")
      }
      case propsex: KafkaLoadPropertiesException => {
        println("KAFKA PRODUCER PROPERTIES NOT FOUND")
      }
    }
    return None
  }

  def loadDefaultConsumerProperties: Option[Properties] = {
    val dprops = new Properties()
    try {
      val inpropsFile = KafkaUtilities.getClass.getResourceAsStream("/DefaultKafkaConsumerProperties.xml")
      if(Option(inpropsFile).isDefined)
        dprops.loadFromXML(inpropsFile)
      else
        throw new KafkaLoadPropertiesException("DefaultKafkaConsumerProperties.xml not found")
      return Option(dprops)
    } catch {
      case ioex: IOException => {
        println("COULD NOT LOAD DEFAULT KAFKA CONSUMER PROPS")
      }
      case propsex: KafkaLoadPropertiesException => {
        println("KAFKA CONSUMER PROPERTIES NOT FOUND")
      }
    }
    return None
  }
}

case class KafkaLoadPropertiesException(message: String) extends Exception

