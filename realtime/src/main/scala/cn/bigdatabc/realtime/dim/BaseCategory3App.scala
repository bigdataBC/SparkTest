package cn.bigdatabc.realtime.dim

import cn.bigdatabc.realtime.bean.BaseCategory3
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
  * 读取商品分类维度数据到Hbase
  */
object BaseCategory3App {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseCategory3App")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ods_base_category3";
    val groupId = "dim_base_category3_group"
    /////////////////////  偏移量处理///////////////////////////
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    var kafkaDS: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      kafkaDS = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      kafkaDS = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangeDS: DStream[ConsumerRecord[String, String]] = kafkaDS.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //转换结构
    val objDS: DStream[BaseCategory3] = offsetRangeDS.map(
      record => {
        val line: String = record.value()
        val baseCategory: BaseCategory3 = JSON.parseObject(line, classOf[BaseCategory3])
        baseCategory
      }
    )
    //保存到Hbase
    import org.apache.phoenix.spark._
    objDS.foreachRDD(
      rdd => {
        rdd.saveToPhoenix("GMALL0523_BASE_CATEGORY3",
          Seq("ID", "NAME", "CATEGORY2_ID" )
          ,new Configuration,Some("node1,node2,node3:2181"))
        OffsetManagerUtil.saveOffset(topic,groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
