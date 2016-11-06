package org.thegreenseek.samples.kafka.fwk

import java.util.Properties
import java.io.{FileInputStream, IOException}
import scala.util.Properties

/**
  * Created by Macphil11 on 12/08/2016.
  * updated 30 Ocy 2016: result is now wrapped in Option
  * @return Some or None
  */
object KafkaUtilities {
  def loadDefaultProperties: Option[Properties] = {
    val dprops = new Properties()
    try {
      dprops.loadFromXML(this.getClass.getResourceAsStream("DefaultKafkaProperties.xml"))
      return Option(dprops)
    } catch {
      case ioex: IOException => {
        println("COULD NOT LOAD DEFAULT KAFKA PROPS")
      }
    }
    return None
  }
}

