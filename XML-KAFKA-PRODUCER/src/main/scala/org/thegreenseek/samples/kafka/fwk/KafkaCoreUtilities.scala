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

/*class KafkaCore (props: Properties) {
  var cprops = props != null ? props : {
    var dprops = new Properties()
  }
=======
class KafkaCore (props: Properties) {
  //var cprops = props != null ? props : {
  //  var dprops = new Properties()
  //}
>>>>>>> Stashed changes


}*/
