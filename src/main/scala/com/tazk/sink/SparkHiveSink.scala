package com.tazk.sink

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * 数据存储到hive
 *
 * @author zap
 * @version 1.0, 2020/05/23
 *
 */
class SparkHiveSink(spark: SparkSession,
                    database: String = "default",
                    table: String,
                    partitionKey: String,
                    partitionValue: String,
                    format: String = "text",
                    deleteTableIfExists: Boolean = false,
                    enableDynamicPartition: Boolean = false,
                    dynamicPartitionKeys: Option[String] = None) extends TazkSink[Dataset[String]] {

  /**
   * 写入目标库
   */
  override def write(dataset: Dataset[String]): Long = {

    // 检查分区键和值是否对应
    if (null != partitionKey && null != partitionValue) {
      if (partitionKey.split(",").length != partitionValue.split(",").length) {
        throw new IllegalArgumentException("分区键和值长度以一致.")
      }
    }

    import spark.implicits._
    // 检查目标表是否存在
    spark.catalog.setCurrentDatabase(database)
    val listTables = spark.catalog.listTables(database)
    if (listTables.mapPartitions(iter => iter.map(_.name == table))
      .filter(x => x).collect().length > 0) {
      if (!deleteTableIfExists) {
        throw new IllegalArgumentException(String.format("[%s]目标表存在", table))
      } else {
        // 删除目标表
        spark.sql(s"drop table $table")
      }
    }

    val mongoCount = spark.sparkContext.longAccumulator("SPARK_IMPORT_MONGO_COUNT")

    dataset.foreachPartition(iter => mongoCount.add(iter.size))

    // 分区写入
    val partitionOption = partitionInfo(partitionKey, partitionValue)
    if (partitionOption.nonEmpty) {
      dataset.write.partitionBy(partitionOption.get).format(format).mode(SaveMode.Overwrite)
        .insertInto(table)
    } else if (partitionOption.isEmpty && !enableDynamicPartition) {
      dataset.write.format(format).mode(SaveMode.Overwrite).saveAsTable(table)
    } else {
      if (!enableDynamicPartition) throw new IllegalArgumentException("动态分区必须启动")
      else {
        if (dynamicPartitionKeys.isEmpty) {
          throw new IllegalArgumentException("当开启动态分区时，动态分区列名必须指定")
        }
        spark.conf.set("hive.exec.dynamici.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.conf.set("hive.exec.max.dynamic.partitions", "100000")
        dataset.write.partitionBy(dynamicPartitionKeys.get.split(","): _*)
          .mode(SaveMode.Overwrite).saveAsTable(table)
      }
    }
    mongoCount.value

  }

  /**
   * 手动指定分区信息
   */
  private def partitionInfo(keys: String, value: String): Option[String] = {
    if (null == keys) None
    else {
      val tuple = keys.split(",").zip(value.split(","))
      Some(tuple.map(tup => s"${tup._1}='${tup._2}'").mkString(","))
    }
  }

}
