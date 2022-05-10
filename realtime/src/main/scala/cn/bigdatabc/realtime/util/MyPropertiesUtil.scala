package cn.bigdatabc.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
  * 读取配置文件的配置项
  */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    print(properties.getProperty("kafka.broker.list"))
  }

  def load(propertiesName: String): Properties = {
    val properties = new Properties()
    properties.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8
    ))
    properties
  }
}
