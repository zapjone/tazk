package com.tazk.core

import org.apache.spark.sql.SparkSession

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/25
 *
 */
private[core] abstract class TazkSparkCore extends TazkCore {

  /**
   * 创建SparkSession
   */
  protected def getOrCreateSparkSession(arguments: SparkImportArguments): SparkSession = {
    val spark = SparkSession.builder()
      .appName(arguments.name)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

}
