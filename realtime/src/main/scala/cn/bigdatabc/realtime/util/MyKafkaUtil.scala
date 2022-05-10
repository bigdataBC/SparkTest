package cn.bigdatabc.realtime.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * 读取kafka的工具类
  */
object MyKafkaUtil {
  // 从配置文件获取配置项
  private val brokerList: String = MyPropertiesUtil.load("config.properties").getProperty("kafka.broker.list")

  // 配置kafka消费参数
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "gmall0523_group",
    "auto.offset.reset" -> "latest",
    //    "auto.offset.reset" -> "earliest",
    // 设为true会在后台自动提交偏移量，弊端是kafka宕机容易丢失数据
    // 设为true不会自动提交偏移量，但需要手动维护偏移量（redis）
    "enable.auto.commit" -> (true: java.lang.Boolean)
    //    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * 创建DStream，返回接收到的输入数据（使用默认的消费者组）
    * @param topic
    * @param ssc
    * @return
    */
  def getKafkaStream(topic: String, ssc: StreamingContext) = {
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  /**
    * 在对Kafka数据进行消费的时候，指定消费者组
    * @param topic
    * @param ssc
    * @param groupId
    * @return
    */
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String) = {
    kafkaParam("group.id") = groupId
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  /**
    * 从指定的偏移量位置读取数据
    * @param topic
    * @param ssc
    * @param offsets
    * @param groupId
    * @return
    */
  def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId:String)  = {
    kafkaParam("group.id") = groupId
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets)
    )
    dStream
  }
}

