package cn.bigdatabc.realtime.dim

import cn.bigdatabc.realtime.bean.ProvinceInfo
import cn.bigdatabc.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka中读取省份数据，保存到Phoenix
  */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ProvinceInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_base_province"
    var groupId = "province_info_group"

    //==============1.从Kafka中读取数据===============
    //1.1获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    //1.2根据偏移量获取数据
    var recordDS: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDS = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDS = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //1.3获取当前批次获取偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDS: DStream[ConsumerRecord[String, String]] = recordDS.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //2.===========保存数据到Phoenix===========
    offsetDS.foreachRDD(
      rdd => {
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map(
          record => {
            //获取省份的json格式字符串
            //将json格式字符串封装为ProvinceInfo对象
            val line: String = record.value()
            val provinceInfo: ProvinceInfo = JSON.parseObject(line, classOf[ProvinceInfo])
            provinceInfo
          }
        )
        import org.apache.phoenix.spark._
        provinceInfoRDD.saveToPhoenix(
          "GMALL0523_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("node1,node2,node3:2181")
        )
        //保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
