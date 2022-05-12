package cn.bigdatabc.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * 用于从Phoenix中查询数据
  * * User_id     if_consumerd
  * *   zs            1
  * *   ls            1
  * *   ww            1
  * *
  * *  期望结果：
  * *  {"user_id":"zs","if_consumerd":"1"}
  * *  {"user_id":"zs","if_consumerd":"1"}
  * *  {"user_id":"zs","if_consumerd":"1"}
  */
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from user_status0523")
    println(list)
  }

  def queryList(sql: String): List[JSONObject] = {
    val rsList = new ListBuffer[JSONObject]
    // 注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    // 建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:node1,node2,node3:2181")
    // 创建数据库操作对象
    val preparedStatement: PreparedStatement = conn.prepareStatement(sql)
    // 执行SQL语句
    val resultSet: ResultSet = preparedStatement.executeQuery()
    val rsMetaData: ResultSetMetaData = resultSet.getMetaData
    // 处理结果集
    while (resultSet.next()) {
      val userStatusJSONObj = new JSONObject()
      //{"user_id":"zs","if_consumerd":"1"}
      for (i <- 1 to rsMetaData.getColumnCount) {
        userStatusJSONObj.put(rsMetaData.getColumnName(i), resultSet.getObject(i))
      }
      rsList.append(userStatusJSONObj)
    }
    // 释放资源
    resultSet.close()
    preparedStatement.close()
    conn.close()
    // 返回查询得到并处理的结果
    rsList.toList
  }
}
