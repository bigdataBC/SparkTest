package cn.bigdatabc.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import cn.bigdatabc.realtime.bean.UserInfo
import cn.bigdatabc.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka中读取用户数据，保存到Phoenix
  */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.1基本环境准备
    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "ods_user_info"
    val groupId = "user_info_group"

    //1.2获取偏移量,根据偏移量获取数据
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDS: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDS = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDS = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //1.3获取本批次消费数据的偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDS: DStream[ConsumerRecord[String, String]] = recordDS.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //1.4对从Kafka中读取的数据进行结构的转换  record(kv)==>UserInfo
    val userInfoDS: DStream[UserInfo] = offsetDS.map(
      record => {
        val line: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(line, classOf[UserInfo])
        //把生日转成年龄
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = sdf.parse(userInfo.birthday)
        val betweenMs = System.currentTimeMillis() - date.getTime

        val age = betweenMs / 1000L / 60L / 60L / 24L / 365L
        if (age < 20) {
          userInfo.age_group = "20岁及以下"
        } else if (age > 30) {
          userInfo.age_group = "30岁以上"
        } else {
          userInfo.age_group = "21岁到30岁"
        }

        if (userInfo.gender == "M") {
          userInfo.gender_name = "男"
        } else {
          userInfo.gender_name = "女"
        }
        userInfo
      }
    )
    //1.5保存到Phoenix中
    userInfoDS.foreachRDD{
      rdd=>{
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix(
          "GMALL0523_USER_INFO",
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("node1,node2,node3:2181")
        )
        //1.6提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
