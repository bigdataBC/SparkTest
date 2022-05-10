package cn.bigdatabc.realtime.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 向Kafka主题中发送数据
  */
object MyKafkaSink {
  private val prop: Properties = MyPropertiesUtil.load("config.properties")
  val brokerList: String = prop.getProperty("kafka.broker.list")
  val kafkaProducer: KafkaProducer[String, String] = null

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", brokerList)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idempotence",(true: java.lang.Boolean))

    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  /**
    * 指定topic发消息
    * @param topic
    * @param msg
    */
  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
    * 指定topic和key发消息
    * @param topic
    * @param key
    * @param msg
    */
  def send(topic: String, key: String, msg: String): Unit = {
    if (kafkaProducer == null) createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }

}
