package cn.bigdatabc.realtime.dim

import cn.bigdatabc.realtime.bean.BaseTrademark
import cn.bigdatabc.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.internal.util.HasClassPath

/**
  * 从Kafka中读取品牌维度数据，保存到Hbase
  */
object BaseTrademarkApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseTrademarkApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ods_base_trademark"
    val groupId = "dim_base_trademark_group"
    /////////////////////  偏移量处理///////////////////////////
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    var kafkaDS: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      kafkaDS = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      kafkaDS = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 取得偏移量步长
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangeDS: DStream[ConsumerRecord[String, String]] = kafkaDS.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // 转换成更方便操作的专用样例类
    val objectDstream: DStream[BaseTrademark] = offsetRangeDS.map(
      record => {
        val line: String = record.value()
        val baseTrademark: BaseTrademark = JSON.parseObject(line, classOf[BaseTrademark])
        baseTrademark
      }
    )
    //保存到Hbase
    import org.apache.phoenix.spark._
    objectDstream.foreachRDD(
      rdd => {
        rdd.saveToPhoenix("GMALL0523_BASE_TRADEMARK",Seq("ID", "TM_NAME"  )
          ,new Configuration,Some("node1,node2,node3:2181"))
        OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
