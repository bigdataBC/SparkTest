package cn.bigdatabc.realtime.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtil {
  // 定义一个连接池对象
  private var jedisPool: JedisPool = null

  /**
    * 创建JedisPool连接池对象
    */
  def build() = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    val redisHost: String = prop.getProperty("redis.host")
    val redisPort: String = prop.getProperty("redis.port")

    val jedisPoolConfig = new JedisPoolConfig
    // 最大连接数
    jedisPoolConfig.setMaxTotal(100)  //最大连接数
    jedisPoolConfig.setMaxIdle(20)   //最大空闲
    jedisPoolConfig.setMinIdle(20)     //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort.toInt)
  }

  /**
    * 获取Jedis客户端
    */
  def getJedisClient() = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient()
    jedis.ping()
  }
}
