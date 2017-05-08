package org.thegreenseek.samples.kafka.fwk

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/**
  * most important classes are in hadoop-common-2.2.9.jar
  * Created by Macphil11 on 05/05/2017.
  */
class HadoopClient private (){

  //first we setup the path for configuration
  var hadoop_conf_path: String = sys.env(Constants.HADOOP_ENV_CONFIGURATION)
  if(hadoop_conf_path == null)
    hadoop_conf_path = sys.env(Constants.HADOOP_HOME) + "/etc/hadoop/"

  //then we setup the hadoop configuration itself
  System.setProperty("HADOOP_USER_NAME", "Macphil11")
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path(hadoop_conf_path + Constants.HADOOP_CORE_SITE_CONF_FILE)
  private val hdfsHDFSSitePath = new Path(hadoop_conf_path + Constants.HADOOP_CORE_HDFS_CONF_FILE)
  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  conf.set("fs.defaultFS", "hdfs://localhost:9000")

  private val fileSystem = FileSystem.get(conf)

  //handle shutdownhook
  sys.ShutdownHookThread {
    if(fileSystem != null && fileSystem.exists(new Path("/")))
      fileSystem.close()
  }

  def getInpuStream(spath:String): InputStream = {
    var path = new Path(spath)
    fileSystem.open(path)
  }

  def close()= {
    fileSystem.close()
  }
}


object HadoopClient {
  val _instance:HadoopClient = new HadoopClient()

  def instance():HadoopClient = {
    _instance
  }
}
