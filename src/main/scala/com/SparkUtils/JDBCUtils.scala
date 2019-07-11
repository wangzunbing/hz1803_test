package com.SparkUtils

import java.util.Properties

import com.config.ConfigManager

/**
  * JDBC
  */
object JDBCUtils {

  def getJdbcProp():(Properties,String)={
    val prop = new Properties()
    prop.put("user",ConfigManager.getProper("jdbc.user"))
    prop.put("password",ConfigManager.getProper("jdbc.password"))
    prop.put("driver",ConfigManager.getProper("jdbc.driver"))
    val jdbcUrl = ConfigManager.getProper("jdbc.url")
    (prop,jdbcUrl)
  }
}
