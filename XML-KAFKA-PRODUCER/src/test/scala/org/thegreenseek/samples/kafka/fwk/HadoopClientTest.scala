package org.thegreenseek.samples.kafka.fwk

import org.scalatest.FunSuite

/**
  * Created by Macphil11 on 08/05/2017.
  */
class HadoopClientTest extends FunSuite {

  test("testGetInpuStream") {

    /*val client = HadoopClient.instance()
    assert(client != null)

    var pathToFile = "hdfs://user/Macphil1/landsat-small.xml"*/

    val inStream = HadoopClient.instance().getInpuStream("/user/Macphil1/landsat-small.xml")
    assert(inStream != null)


  }

}
