package com.tazk.sink

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/7/30
 *
 */
class SparkGreenPlumSink(spark: SparkSession,
                         url: String,
                         database: String,
                         collection: String,
                         username: String,
                         password: String) extends TazkSink[Dataset[Row], (Long, Long, Long)] {

  /**
   * 写入目标库
   *
   * @return 返回到信息，一般返回其条数
   */
  override def write(t: Dataset[Row]): (Long, Long, Long) = {
    ???
  }

}
