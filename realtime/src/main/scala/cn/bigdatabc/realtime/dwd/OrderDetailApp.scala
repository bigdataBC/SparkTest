package cn.bigdatabc.realtime.dwd

import cn.bigdatabc.realtime.bean.OrderDetail
import cn.bigdatabc.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka的ods_order_detail主题中，读取订单明细数据
  */
object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderDetailApp").setMaster("local[4")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ods_order_detail"
    val groupId = "order_detail_group"
    //从redis中读取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    //通过偏移量到Kafka中获取数据
    var kafkaDS: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      kafkaDS = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      kafkaDS = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangeDS: DStream[ConsumerRecord[String, String]] = kafkaDS.transform(
      rdd => {
        //周期性在driver中执行
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    val orderDetailDS: DStream[OrderDetail] = offsetRangeDS.map(
      record => {
        //提取数据
        //订单处理  转换成更方便操作的专用样例类
        val line: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(line, classOf[OrderDetail])
        orderDetail
      }
    )
    orderDetailDS.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}
