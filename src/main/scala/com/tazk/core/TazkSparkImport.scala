package com.tazk.core

import com.tazk.internal.Logging
import com.tazk.sink.SparkHiveSink
import com.tazk.source.SparkMongoSource
import com.tazk.util.Utils
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession

/**
 * 使用Spark导入Mongo数据到hive中
 *
 * @author zap
 * @version 1.0, 2020/05/24
 *
 */
object TazkSparkImport extends TazkImport with Logging {


  lazy private val sparkMongo = (spark: SparkSession, arguments: SparkImportArguments) => new SparkMongoSource(spark,
    arguments.mongoUri,
    arguments.mongoDatabase,
    arguments.mongoCcollection,
    arguments.mongoUserName,
    arguments.mongoPassword,
    arguments.mongoCondition,
    arguments.mongoConditionEncrypt,
    arguments.mongoCamelConvert,
    arguments.mongoOtherConf)

  lazy private val sparkHive = (spark: SparkSession, arguments: SparkImportArguments) => new SparkHiveSink(spark,
    arguments.hiveDatabase,
    arguments.hiveTable,
    arguments.hivePartitionKey,
    arguments.hivePartitionValue,
    arguments.hiveFormat,
    arguments.hiveDeleteTableIfExists,
    arguments.hiveEnableDynamicPartition,
    arguments.hiveDynamicPartitionKeys)


  /**
   * 开始导入操作
   */
  override def main(args: Array[String]): Unit = {
    if (null == args || args.length <= 0) {
      throw new IllegalArgumentException("参数不能为空")
    }
    // 解析传递过来参数
    val arguments = Utils.parseObject[SparkImportArguments](new String(Base64.decodeBase64(args(0))),
      classOf[SparkImportArguments])


    // 创建SparkSession
    val spark = getOrCreateSparkSession(arguments)

    log.info("开始读取hive数据并转换成dataset")
    val mongoDS = sparkMongo(spark, arguments).read()
    val writeCount = sparkHive(spark, arguments).write(mongoDS)
    log.info(s"导入完成，共导入${writeCount}条数据")

    spark.stop
  }


  /**
   * 创建SparkSession
   */
  private def getOrCreateSparkSession(arguments: SparkImportArguments): SparkSession = {
    val spark = SparkSession.builder()
      .appName(arguments.name)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }


}
