package com.tazk.core

import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/25
 *
 */
private[core] abstract class TazkSparkCore extends TazkCore {

  /**
   * 检查输入参数
   */
  protected def checkInputArgs(args: Array[String]): String = {
    if (null == args || args.length <= 0) {
      throw new IllegalArgumentException("参数不能为空")
    }
    new String(Base64.decodeBase64(args(0)))
  }

  /**
   * 创建SparkSession
   */
  protected def getOrCreateSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

}
