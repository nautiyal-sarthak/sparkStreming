/**
  * The following program can be compiled and run using SBT
  * Wrapper scripts have been provided with this
  * The following script can be run to compile the code
  * ./compile.sh
  * *
  * The following script can be used to run this application in Spark. The second command line argument of value 1 is very important. This is to flag the shipping of the kafka jar files to the Spark cluster
  * ./submit.sh com.packtpub.sfb.KafkaStreamingApps 1
  **/
package com.cn.otds.snow

import java.io.{File, InputStream}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SnowStreamingApps {

  final val LOGGER: Logger = Logger.getLogger(getClass)

  def init(args: Array[String]): Unit = {
    LOGGER.info("Starting " + getClass.getSimpleName)
  }

  def main(args: Array[String]) {

    init(args)

    if (args.length == 0 || args(0).isEmpty) {
      println("Profile must be provided")
      System.exit(1)
    }

    val profile = args(0)
    val configFile : InputStream = getClass.getResourceAsStream("/config-" + profile + ".properties")

    if (configFile == null) {
      println("Configuration file not found: config-" + profile + ".properties")
      System.exit(1)
    }

    val configProps = new java.util.Properties
    configProps.load(configFile)

    // Variables used for creating the Kafka stream
    // Message group name
    val checkpointLocation = configProps.get("checkpointLocation").toString
    val src_server = configProps.get("src.bootstrap.servers").toString
    val src_topic = configProps.get("src.topic").toString
    val src_protocol = configProps.get("src.security.protocol").toString
    val src_startingOffsets = configProps.get("src.startingOffsets").toString

    val dest_server = configProps.get("dest.bootstrap.servers").toString
    val dest_topic = configProps.get("dest.topic").toString
    val dest_protocol = configProps.get("dest.security.protocol").toString
    val dest_outputMode = configProps.get("dest.outputMode").toString

    LOGGER.debug("checkpointLocation : " + checkpointLocation)
    LOGGER.debug("src.bootstrap.servers : " + src_server)
    LOGGER.debug("src.topic : " + src_topic)
    LOGGER.debug("src.security.protocol : " + src_protocol)
    LOGGER.debug("src.startingOffsets : " + src_startingOffsets)

    LOGGER.debug("dest.bootstrap.servers : " + dest_server)
    LOGGER.debug("dest.topic : " + dest_topic)
    LOGGER.debug("dest.security.protocol : " + dest_protocol)

    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .config("spark.task.maxFailures ", 6)
      .config("spark.streaming.backpressure.enabled", value = true)
      .config("spark.streaming.receiver.maxRate", 100)
      .config("spark.streaming.kafka.maxRatePerPartition", 100)
      //.master("local")
      .getOrCreate()

    // Set the check point directory for saving the data to recover when there is a crash
    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpointLocation)
    System.setProperty("java.security.auth.login.config", "client_jaas.conf")
    //System.setProperty("java.security.auth.login.config", "C:\\Apps\\hdfs_conf\\client_jaas.conf")

    LOGGER.debug("Starting reading Kafka stream")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", src_server)
      .option("kafka.security.protocol", src_protocol)
      .option("subscribe", src_topic)
      .option("startingOffsets", src_startingOffsets)
      .load()

    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    ds
      .writeStream // use `write` for batch, like DataFrame
      .format("kafka")
      .outputMode(dest_outputMode)
      .option("kafka.security.protocol", dest_protocol)
      .option("kafka.bootstrap.servers", dest_server)
      .option("topic", dest_topic)
      .start().awaitTermination()

  }
}
