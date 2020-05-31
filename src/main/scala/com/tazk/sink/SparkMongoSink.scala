package com.tazk.sink

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import com.tazk.deploy.TazkMongoUpdateModeAction
import com.tazk.deploy.TazkMongoUpdateModeAction.TazkMongoUpdateModeAction
import com.tazk.util.Utils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.bson.Document

import scala.collection.mutable
import java.lang.{Object => JObject}
import java.util.{Map => JMap}
import scala.collection.JavaConverters._

/**
 *
 * 更新数据到mongo，
 * 有三种模式：
 * allowInsert: 只增加
 * allowUpdate: 增加和更新
 * allowDelete: 增加和更新并删除
 *
 * @author zap
 * @version 1.0, 2020/05/30
 *
 */
class SparkMongoSink(spark: SparkSession,
                     uri: String,
                     database: String,
                     collection: String,
                     username: String,
                     password: String,
                     otherConf: mutable.HashMap[String, String],
                     updateMode: TazkMongoUpdateModeAction,
                     updateKey: Option[String],
                     ignoreUpdateKey: Option[String],
                     camelConvert: Boolean) extends TazkSink[Dataset[Row], (Long, Long, Long)] {

  /**
   * 写入目标库
   *
   * @return 返回写入成功条数
   */
  override def write(dataset: Dataset[Row]): (Long, Long, Long) = {

    // mongo配置信息
    val writeConfig: WriteConfig = WriteConfig(Map(
      "uri" -> Utils.buildMongoUri(uri, username, password),
      "database" -> database,
      "collection" -> collection
    ) ++ otherConf)

    updateMode match {
      case TazkMongoUpdateModeAction.ALLOW_INSERT => (insert(dataset, writeConfig), 0, 0)
      case TazkMongoUpdateModeAction.ALLOW_UPDATE => ???
      case TazkMongoUpdateModeAction.ALLOW_DELETE => ???
      case _ => throw new IllegalArgumentException(s"[$updateMode]不支持到类型")
    }
  }

  /**
   * 直接添加数据到mongo
   *
   * @param dataset     Dataset[Row]数据
   * @param writeConfig mongo配置信息
   * @return 成功插入到mongo的条数
   */
  private def insert(dataset: Dataset[Row], writeConfig: WriteConfig): Long = {
    val insertMongoCount = spark.sparkContext.longAccumulator("INSERT_MONGO_COUNT")
    dataset.foreachPartition(rowPartition => {
      // 创建Mongoecotr
      val mongoConnector = MongoConnector(writeConfig.asOptions)
      // 获取dataset的所有列名,如果需要将列名进行转换，则进行转换
      val colNames = if (camelConvert) {
        dataset.columns.map(Utils.line2Hump)
      } else dataset.columns

      if (rowPartition.nonEmpty) {
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          rowPartition.foreach(row => {
            // 遍历row并转换成Document
            val valMap: JMap[String, JObject] = (for (index <- 0 until row.size) yield {
              colNames(index) -> row.get(index).asInstanceOf[JObject]
            }).toMap.asJava
            val doc = new Document(valMap)
            collection.insertOne(doc)
            insertMongoCount.add(1)
          })
        })
      }
    })
    insertMongoCount.value
  }

}
