package com.tazk.core

import com.tazk.deploy.TazkMongoUpdateModeAction
import com.tazk.internal.Logging
import com.tazk.sink.SparkMongoSink
import com.tazk.source.SparkHiveSource
import com.tazk.util.Utils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 *
 * 使用Spark将hive根据模式将数据导出到mongo
 *
 * @author zhangap 
 * @version 1.0, 2020/5/25
 *
 */
object TazkSparkExport extends TazkSparkCore with Logging {

  lazy private val sparkHive = (spark: SparkSession, arguments: SparkExportArguments) => new SparkHiveSource(spark,
    arguments.hiveDatabase,
    arguments.hiveTable,
    arguments.hiveCondition.getOrElse("1=1"))

  lazy private val sparkMongo = (spark: SparkSession, arguments: SparkExportArguments) => new SparkMongoSink(spark,
    arguments.mongoUri,
    arguments.mongoDatabase,
    arguments.mongoCollection,
    arguments.mongoUserName,
    arguments.mongoPassword,
    arguments.mongoOtherConf.getOrElse(mutable.HashMap()),
    TazkMongoUpdateModeAction.findOf(arguments.mongoUpdateMode),
    arguments.mongoUpdateKey,
    arguments.mongoIgnoreUpdateKey,
    arguments.mongoCamelConvert)

  /**
   * 导出操作
   */
  override def main(args: Array[String]): Unit = {
    // 解析传递过来参数
    val arguments = Utils.parseObject[SparkExportArguments](checkInputArgs(args))
    val spark = getOrCreateSparkSession(arguments.name)

    log.info("开始读取hive数据，准备写入到mongo")
    val hiveDS = sparkHive(spark, arguments).read()
    val (insertCount, updateCount, deleteCount) = sparkMongo(spark, arguments).write(hiveDS)
    log.info(
      s"""
         |导出完成，共导出${insertCount + updateCount + deleteCount}条数据. \n
         |新增条数: $insertCount \n
         |更新条数: $updateCount \n
         |删除条数: $deleteCount
         |""".stripMargin)

    spark.stop()
  }


}
