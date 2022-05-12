package cn.bigdatabc.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import cn.bigdatabc.realtime.bean.{OrderInfo, UserStatus}
import cn.bigdatabc.realtime.util._
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka中读取订单数据，并对其进行处理
  */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_order_info"
    var groupId = "order_info_group"
    //===================1.从Kafka主题中读取数据====================
    //从Redis获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    //根据偏移量是否存在决定从什么位置开始读取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前批次处理的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val kafkaDS: DStream[ConsumerRecord[String, String]] = recordDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //对DS的结构进行转换   ConsumerRecord[k,v] ===>value:jsonStr ==>OrderInfo
    val orderInfoDS: DStream[OrderInfo] = kafkaDS.map(
      record => {
        //获取json格式字符串
        //将json格式字符串转换为OrderInfo对象
        //2020-10-27 14:30:20
        val line: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(line, classOf[OrderInfo])
        val create_time: String = orderInfo.create_time
        val timeArr: Array[String] = create_time.split(" ")
        orderInfo.create_date = timeArr(0)
        orderInfo.create_hour = timeArr(1).split(":")(0)
        orderInfo
      }
    )
//    orderInfoDS.print(10)


    //===================2.判断是否为首单  ====================
//    //方案1   对于每条订单都要执行一个sql，sql语句过多
//    orderInfoDS.map(
//      orderInfo => {
//        //获取用户id
//        //根据用户id到Phoenix中查询是否下单过
//        val userId: Long = orderInfo.user_id
//        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(s"select user_id,if_consumed from user_status0523 where user_id='${userId}'")
//        if (userStatusList != null && userStatusList.size > 0) {
//          orderInfo.if_first_order = "0"
//        } else {
//          orderInfo.if_first_order = "1"
//        }
//        orderInfo
//      }
//    )


    //===================2.判断是否为首单  ====================
    //方案2  以分区为单位，将整个分区的数据拼接一条SQL进行一次查询
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDS.mapPartitions(
      orderInfoIterator => {
        //当前一个分区中所有的订单的集合
        val orderInfoList: List[OrderInfo] = orderInfoIterator.toList
        //获取当前分区中获取下订单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //根据用户集合到Phoenix中查询，看看哪些用户下过单   坑1  字符串拼接
        var sql: String = s"select user_id,if_consumed from user_status0523 where user_id in('${userIdList.mkString("','")}')";
        //执行sql从Phoenix获取数据
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //获取消费过的用户id   坑2  大小写
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          //坑3    类型转换
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoIterator
      }
    )
    //orderInfoWithFirstFlagDStream.print(1000)


    //===================3.维护首单用户状态|||保存订单到ES中  ====================
    //如果当前用户为首单用户（第一次消费），那么我们进行首单标记之后，应该将用户的消费状态保存到Hbase中，等下次这个用户再下单的时候，就不是首单了
    orderInfoWithFirstFlagDStream.foreachRDD(
      rdd => {
        // 优化
        rdd.cache()
        //3.1 维护首单用户状态
        //将首单用户过滤出来
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
        //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段数必须要一致
        val userStatusRDD: RDD[UserStatus] = firstOrderRDD.map(
          orderInfo => {
            UserStatus(orderInfo.user_id.toString, "1")
          }
        )
        import org.apache.phoenix.spark._
        userStatusRDD.saveToPhoenix(
          "USER_STATUS0523",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration, Some("node1,node2,node3:2181"))

        //3.2保存订单数据到ES中
        rdd.foreachPartition{
          orderInfoItr=>{
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall0523_order_info_" + dateStr)

            //3.4写回到Kafka
            for ((orderInfoId,orderInfo) <- orderInfoList) {
              MyKafkaSink.send("dwd_order_info",JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }

          }
        }
        //3.3提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
