package com.Spark_DW

import com.Constans.Constan
import com.SparkUtils.JDBCUtils
import com.config.ConfigManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.slf4j.LoggerFactory

/**
  * dwd导入dws
  */
object DWS_DM {
  def main(args: Array[String]): Unit = {

    val hiveContext = SparkSession.builder().appName("SPARK_APP_NAME_USER").master(Constan.SPARK_LOACL).enableHiveSupport().config("spark.debug.maxToStringFileds","100").getOrCreate()
    // 加载相应的语句
    val sql = ConfigManager.getProper(args(0))
    if(sql == null){
      LoggerFactory.getLogger("SparkLogger")
        .debug("提交的表名参数有问题！请重新设置。。。")
    }else{
      // 处理SQL内部的占位符
      val finalSql = sql.replace("?",args(1))
      // 运行SQL
      val df = hiveContext.sql(sql)
      // 处理配置参数
      val mysqlTableName = args(0).split("\\.")(1)
      val hiveTableName = args(0)
      val jdbcProp = JDBCUtils.getJdbcProp()._1
      val jdbcUrl = JDBCUtils.getJdbcProp()._2
      // 存入MySQL
      df.show()
       df.write.mode(SaveMode.Append).jdbc(jdbcUrl,mysqlTableName,jdbcProp)
      // 存入Hive
//      df.write.mode(SaveMode.Overwrite).insertInto(hiveTableName)
    }
  }
}