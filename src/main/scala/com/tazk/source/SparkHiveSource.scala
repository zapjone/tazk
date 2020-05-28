package com.tazk.source

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Spark读取hive数据做为数据源
 *
 * @author zhangap 
 * @version 1.0, 2020/5/28
 *
 */
class SparkHiveSource(spark: SparkSession,
                      hiveDatabse: String = "default",
                      hiveTable: String,
                      hiveCondition: String = "1=1") extends TazkSource[Dataset[Row]] {
  /**
   * 读取Hive数据
   */
  override def read(): Dataset[Row] = {
    spark.table(s"$hiveDatabse.$hiveTable").where(hiveCondition)
  }

}
