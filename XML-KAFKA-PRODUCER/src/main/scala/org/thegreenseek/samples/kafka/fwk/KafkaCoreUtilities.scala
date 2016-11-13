package org.thegreenseek.samples.kafka.fwk

import java.util.Properties
import java.io.{FileInputStream, IOException}

/**
  * Created by Macphil11 on 12/08/2016.
  * updated 30 Ocy 2016: result is now wrapped in Option
  * @return Some or None
  */
object KafkaUtilities {
  def loadDefaultProperties: Option[Properties] = {
    val dprops = new Properties()
    try {
        val inpropsFile = KafkaUtilities.getClass.getResourceAsStream("/DefaultKafkaProperties.xml")
        if(Option(inpropsFile).isDefined)
          dprops.loadFromXML(inpropsFile)
        else
          throw new KafkaLoadPropertiesException("DefaultKafkaProperties.xml not found")
      return Option(dprops)
    } catch {
      case ioex: IOException => {
        println("COULD NOT LOAD DEFAULT KAFKA PROPS")
      }
      case propsex: KafkaLoadPropertiesException => {
        println("KAFKA PROPERTIES NOT FOUND")
      }
    }
    return None
  }
}

case class KafkaLoadPropertiesException(message: String) extends Exception

