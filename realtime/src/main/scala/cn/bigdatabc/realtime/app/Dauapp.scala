package cn.bigdatabc.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import cn.bigdatabc.realtime.bean.DauInfo
import cn.bigdatabc.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * 日活
 */
object Dauapp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        /**
          * 消费Kafka数据基本实现
          */
        var topic:String = "gmall_start_0523"
        var groupId:String = "gmall_dau_0523"
        //从Redis中获取Kafka分区偏移量
        val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

        var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
        if(offsetMap!=null && offsetMap.size >0){
            //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
            recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
        }else{
            //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
            recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
        }

        //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

        val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
            rdd => {
                //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
            record => {
                val jsonString: String = record.value()
                //将json格式字符串转换为json对象
                val jsonObject: JSONObject = JSON.parseObject(jsonString)
                //从json对象中获取时间戳
                val ts: lang.Long = jsonObject.getLong("ts")
                //将时间戳转换为日期和小时  2020-10-21 16
                val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
                val dateStrArr: Array[String] = dateStr.split(" ")
                var dt = dateStrArr(0)
                var hr = dateStrArr(1)
                jsonObject.put("dt", dt)
                jsonObject.put("hr", hr)
                jsonObject
            }
        }
        //jsonObjDStream.print(1000)


        /*
        //通过Redis   对采集到的启动日志进行去重操作  方案1  采集周期中的每条数据都要获取一次Redis的连接，连接过于频繁
        //redis 类型 set    key：  dau：2020-10-23    value: mid    expire   3600*24
        val filteredDStream: DStream[JSONObject] = jsonObjDStream.filter {
          jsonObj => {
            //获取登录日期
            val dt = jsonObj.getString("dt")
            //获取设备id
            val mid = jsonObj.getJSONObject("common").getString("mid")
            //拼接Redis中保存登录信息的key
            var dauKey = "dau:" + dt
            //获取Jedis客户端
            val jedis: Jedis = MyRedisUtil.getJedisClient()
            //从redis中判断当前设置是否已经登录过
            val isFirst: lang.Long = jedis.sadd(dauKey, mid)
            //设置key的失效时间
            if (jedis.ttl(dauKey) < 0) {
              jedis.expire(dauKey, 3600 * 24)
            }
            //关闭连接
            jedis.close()

            if (isFirst == 1L) {
              //说明是第一次登录
              true
            } else {
              //说明今天已经登录过了
              false
            }
          }
        }
        */
        //通过Redis   对采集到的启动日志进行去重操作  方案2  以分区为单位对数据进行处理，每一个分区获取一次Redis的连接
        //redis 类型 set    key：  dau：2020-10-23    value: mid    expire   3600*24
        val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
            jsonObjItr => { //以分区为单位对数据进行处理
                //每一个分区获取一次Redis的连接
                val jedis: Jedis = MyRedisUtil.getJedisClient()
                //定义一个集合，用于存放当前分区中第一次登陆的日志
                val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
                //对分区的数据进行遍历
                for (jsonObj <- jsonObjItr) {
                    //获取日期
                    val dt = jsonObj.getString("dt")
                    //获取设备id
                    val mid = jsonObj.getJSONObject("common").getString("mid")
                    //拼接操作redis的key
                    var dauKey = "dau:" + dt
                    val isFirst = jedis.sadd(dauKey, mid)
                    //设置key的失效时间
                    if (jedis.ttl(dauKey) < 0) {
                        jedis.expire(dauKey, 3600 * 24)
                    }
                    if (isFirst == 1L) {
                        //说明是第一次登录
                        filteredList.append(jsonObj)
                    }
                }
                jedis.close()
                filteredList.toIterator
            }
        }

        //filteredDStream.count().print()

        //将数据批量的保存到ES中
        filteredDStream.foreachRDD{
            rdd=>{
                //以分区为单位对数据进行处理
                rdd.foreachPartition{
                    jsonObjItr=>{
                        val dauInfoList: List[(String,DauInfo)] = jsonObjItr.map {
                            jsonObj => {
                                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                                val dauInfo = DauInfo(
                                    commonJsonObj.getString("mid"),
                                    commonJsonObj.getString("uid"),
                                    commonJsonObj.getString("ar"),
                                    commonJsonObj.getString("ch"),
                                    commonJsonObj.getString("vc"),
                                    jsonObj.getString("dt"),
                                    jsonObj.getString("hr"),
                                    "00",
                                    jsonObj.getLong("ts")
                                )
                                (dauInfo.mid,dauInfo)
                            }
                        }.toList

                        //将数据批量的保存到ES中
                        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                        MyESUtil.bulkInsert(dauInfoList,"gmall0523_dau_info_" + dt)
                    }
                }
                //提交偏移量到Redis中
                OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
            }
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
