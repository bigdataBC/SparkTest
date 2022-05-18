package cn.bigdatabc.realtime.dws

import java.lang
import java.util.Properties

import cn.bigdatabc.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import cn.bigdatabc.realtime.util._
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 从Kafka的DWD层，读取订单和订单明细数据
  * 注意：如果程序数据的来源是Kafka，在程序中如果触发多次行动操作，应该进行缓存
  */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    //===============1.从Kafka中获取数据================
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"

    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    // 获取偏移量
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)
    // 根据偏移量从指定位置开始获取Kafka数据
    var orderInfoKafkaDS: InputDStream[ConsumerRecord[String, String]] = null
    var orderDetailKafkaDS: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0) {
      orderInfoKafkaDS = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, orderInfoGroupId)
    } else {
      orderInfoKafkaDS = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }
    if (orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0) {
      orderDetailKafkaDS = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, orderDetailGroupId)
    } else {
      orderDetailKafkaDS = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }
    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    var orderInfoOffsetRange = Array.empty[OffsetRange]
    var orderDetailOffsetRange = Array.empty[OffsetRange]
    val orderInfoRangeDS: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDS.transform(
      rdd => {
        orderInfoOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    val orderDetailRangeDS: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDS.transform(
      rdd => {
        orderDetailOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // 转换成对应bean对象
    val orderInfoDS: DStream[OrderInfo] = orderInfoRangeDS.map(
      record => {
        val line: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(line, classOf[OrderInfo])
        orderInfo
      }
    )
    val orderDetailDS: DStream[OrderDetail] = orderDetailRangeDS.map(
      record => {
        val line: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(line, classOf[OrderDetail])
        orderDetail
      }
    )
    //===============2.双流Join================
    //开窗
    val orderInfoWindowDS: DStream[OrderInfo] = orderInfoDS.window(Seconds(20), Seconds(5))
    val orderDetailWindowDS: DStream[OrderDetail] = orderDetailDS.window(Seconds(20), Seconds(5))

    //转换为kv结构 orderID,obj
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDS.map(
      orderInfo => {
        (orderInfo.id, orderInfo)
      }
    )
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDS.map(
      orderDetail => {
        (orderDetail.order_id, orderDetail)
      }
    )
    //双流join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)
    //去重  Redis   type:set    key: order_join:[orderId]   value:orderDetailId  expire :600
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions(
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        val jedisClient: Jedis = MyRedisUtil.getJedisClient()
        val orderWideList = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val orderKey: String = "order_join:" + orderId
          val isNotExists: lang.Long = jedisClient.sadd(orderKey, orderDetail.id.toString)
          jedisClient.expire(orderKey, 600)
          if (isNotExists == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedisClient.close()
        orderWideList.toIterator
      }
    )
    //orderWideDStream.print(1000)

    //===============3.实付分摊================
    val orderWideSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions(
      orderWideItr => {
        val orderWideList: List[OrderWide] = orderWideItr.toList
        //获取Jedis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        for (orderWide <- orderWideList) {
          //3.1从Redis中获取明细累加(明细中 Σ商品价格order_price * 商品数量sku_num)
          //type:String		 key:	order_origin_sum:[order_id]		value:			expire:	600
          val orderOriginSumKey = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0D
          val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
          //注意：从Redis中获取字符串，都要做非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            orderOriginSum = orderOriginSumStr.toDouble
          }
          //3.2从Reids中获取实付分摊累加和(明细中 ∑已经计算完成的明细实付分摊金额)
          //type:String		 key:	order_split_sum:[order_id]		value:			expire:600
          val orderSplitSumKey = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0D
          val orderSplitSumStr: String = jedis.get(orderSplitSumKey)
          if (orderSplitSumStr != null && orderOriginSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }
          //3.3判断是否为最后一条  计算实付分摊
          //如何判断是最后一条明细
          //商品价格order_price * 商品数量sku_num = original_total_amount - Σ 商品价格order_price * 商品数量sku_num
          /**
            * if(是最后一条明细){
            * final_detail_amount = final_total_amount - ∑已经计算完成的明细实付分摊金额
            * }else{
            * final_detail_amount = final_total_amount * (order_price * sku_num)/original_total_amount
            * }
            */
          val detailAmount: Double = orderWide.sku_price * orderWide.sku_num
          if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          } else {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100) / 100d
          }
          //3.4更新Redis中的值
          val newOrderOriginSum = orderOriginSum + detailAmount
          jedis.setex(orderOriginSumKey, 600, newOrderOriginSum.toString)
          var newOrderSplitSum = orderSplitSum + orderWide.final_detail_amount
          jedis.setex(orderSplitSumKey, 600, newOrderSplitSum.toString)
        }
        //关闭连接
        jedis.close()
        orderWideList.toIterator
      }
    )
    orderWideSplitDStream.print(1000)
    orderWideSplitDStream.cache()
    println("---------------------------------")
    orderWideSplitDStream.print(1000)
    //向ClickHouse中保存数据
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("spark_sql_orderWide").getOrCreate()

    //对DS中的RDD进行处理
    import spark.implicits._
    orderWideSplitDStream.foreachRDD{
      rdd=>{
        val prop: Properties = MyPropertiesUtil.load("jdbc.properties")
        //        val prop = new Properties
        //        prop.put("username", "default")
        //        prop.put("password", "123456")
        //rdd.cache()
        val df: DataFrame = rdd.toDF
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://node1:8123/default","t_order_wide_0523",prop)


        //将数据写回到Kafka dws_order_wide
        rdd.foreach{
          orderWide=>{
            MyKafkaSink.send("dws_order_wide",JSON.toJSONString(orderWide,new SerializeConfig(true)))
          }
        }

        //提交偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRange)
        OffsetManagerUtil.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRange)
      }
    }

    MyKafkaSink.closeKafkaProducer
    ssc.start()
    ssc.awaitTermination()

  }
}
