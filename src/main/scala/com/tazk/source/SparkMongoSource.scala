package com.tazk.source

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bson.Document

/**
 *
 * 从mongo中读取数据
 *
 * @author zap
 * @version 1.0, 2020/05/23
 *
 */
class SparkMongoSource(spark: SparkSession,
                       uri: String,
                       database: String,
                       collection: String,
                       userName: String,
                       password: String,
                       condition: Option[String],
                       conditionEncrypt: String,
                       otherConf: Map[String, String]) extends TazkSource[Dataset[String]] {

  private val YARN_MASTER = "yarn"
  private val CLUSTER_MODE = "cluster"

  /**
   * 读取数据
   */
  override def read(): Dataset[String] = {
    // 验证输入的类型是否需要进行加密
    val sparkMaster = spark.conf.get("spark.master")
    // 获取真实的mongo条件
    val realMongoCondition = if (YARN_MASTER == sparkMaster) {
      val deployMode = spark.conf.get("spark.submit.deployMode")
      if (CLUSTER_MODE == deployMode) {
        if (condition.nonEmpty && (null == conditionEncrypt || "base64" != conditionEncrypt)) {
          throw new IllegalArgumentException("当使用yarn集群模式运行时，必须将条件进行加密处理，且当前加密类型只支持base64")
        } else {
          Some(new String(Base64.decodeBase64(condition.get)))
        }
      } else condition
    } else condition

    // mongo参数配置
    val mongoConfig = ReadConfig(Map(
      "uri" -> appenduri(),
      "database" -> database,
      "collection" -> collection
    ) ++ otherConf)

    import spark.implicits._

    // 读取mongo数据，如果有查询条件，则进行条件查询
    val mongoRDD = MongoSpark.load(spark.sparkContext, mongoConfig)
    if (realMongoCondition.nonEmpty) {
      mongoRDD.withPipeline(Seq(Document.parse(realMongoCondition.get)))
        .mapPartitions(_.map(_.toJson())).toDS
    } else mongoRDD.mapPartitions(_.map(_.toJson())).toDS()
  }


  /**
   * 拼接mongo地址
   */
  private def appenduri(): String = {
    val mongoPrefix = "mongodb://"
    if (uri.startsWith(mongoPrefix)) {
      // 是否包含用户名和密码
      val withUser = if (null != userName && !uri.contains(userName)) {
        val hostIndex = uri.indexOf("@")
        val index = if (-1 == hostIndex) uri.indexOf("//") + 1 else hostIndex + 1
        s"$mongoPrefix$userName@${uri.substring(index + 1)}"
      } else uri
      if (null != password && !uri.contains(password)) {
        val index = withUser.indexOf("@")
        val prefix = withUser.substring(0, index)
        val suffix = withUser.substring(index)
        s"$prefix:$password$suffix"
      } else withUser
    } else s"$mongoPrefix$userName${if (null != password) s":$password" else ""}@$uri"
  }

}
