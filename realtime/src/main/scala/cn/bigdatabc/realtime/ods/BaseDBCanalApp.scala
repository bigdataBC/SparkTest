package cn.bigdatabc.realtime.ods

import cn.bigdatabc.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka中读取数据，根据表名进行分流处理（canal）
  */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    // 配置Spark配置项和流处理上下文
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[1]")
    val sc = new StreamingContext(conf, Seconds(5))

    // kafka数据源topic和消费者团队名
    var topic = "gmall0523_db_c"
    var groupId = "base_db_canal_group"

    // 从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // Redis获取不为空则从指定的偏移量位置开始消费，否则从最新位置消费
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, sc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, sc, groupId)
    }

    //获取当前批次读取的Kafka主题中偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //对接收到的数据进行结构的转换，ConsumerRecord[String,String(jsonStr)]====>jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map(
      record => {
        //获取json格式的字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    )
    //测试
    jsonObjDStream.print(10)

    //分流：根据不同的表名，将数据发送到不同的kafka主题中去
    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsobObj => {
            //获取操作类型,因为是首单分析，所以只需对INSERT数据操作即可
            val operationType: String = jsobObj.getString("type")
            if (operationType.equals("INSERT")) {
              //获取表名
              val tableName: String = jsobObj.getString("table")
              //获取操作数据
              val datas: JSONArray = jsobObj.getJSONArray("data")
              //拼接目标topic名称
              var topicName = "ods_" + tableName
              //对data数组进行遍历
              import scala.collection.JavaConverters._
              for (dataJSON <- datas.asScala) {
                //根据表名将数据发送到不同的主题中去
                MyKafkaSink.send(topicName, dataJSON.toString)
              }
            }
          }
        )

        //提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    )
    // start
    sc.start()
    sc.awaitTermination()
  }
}
