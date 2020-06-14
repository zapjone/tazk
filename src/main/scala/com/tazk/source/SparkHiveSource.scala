package com.tazk.source

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

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
                      hiveIgnoreExportKey: Option[String],
                      hiveCondition: String = "1=1") extends TazkSource[Dataset[Row]] {

  import spark.implicits._

  /**
   * 读取Hive数据
   */
  override def read(): Dataset[Row] = {
    val hiveData = spark.table(s"$hiveDatabse.$hiveTable").where(hiveCondition)
    if (hiveIgnoreExportKey.nonEmpty) {
      // 忽略导出的key
      hiveData.select(selectExpr(hiveData.columns, hiveIgnoreExportKey.get.split(",")): _*)
    } else hiveData
  }

  /**
   * 排除需要导出的字段
   *
   * @param columns    当前所有字段
   * @param ignoreKeys 需要忽略的字段
   * @return 忽略后的所有字段,即需要查询的key
   */
  private def selectExpr(columns: Array[String], ignoreKeys: Array[String]): Array[Column] = {
    columns.filterNot(ignoreKeys.contains(_)).map(c => $"$c")
  }

}
