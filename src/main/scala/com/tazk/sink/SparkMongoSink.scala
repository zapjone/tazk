package com.tazk.sink

import com.mongodb.client.MongoCollection
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.tazk.deploy.TazkMongoUpdateModeAction
import com.tazk.deploy.TazkMongoUpdateModeAction.TazkMongoUpdateModeAction
import com.tazk.util.Utils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.bson.Document

import scala.collection.mutable
import java.lang.{Object => JObject}
import java.util.{Map => JMap, List => JList}

import com.tazk.common.TazkCommon
import org.apache.spark.util.LongAccumulator

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
                     camelConvert: Boolean)
  extends TazkSink[Dataset[Row], (Long, Long, Long)] with TazkCommon {

  import spark.implicits._

  /**
   * 写入目标库
   *
   * @return 返回写入成功条数
   */
  override def write(dataset: Dataset[Row]): (Long, Long, Long) = {

    // mongo配置信息
    val mongoConfigMap = Map(
      "uri" -> Utils.buildMongoUri(uri, username, password),
      "database" -> database,
      "collection" -> collection
    ) ++ otherConf
    val readConfig: ReadConfig = ReadConfig(mongoConfigMap)
    val writeConfig: WriteConfig = WriteConfig(mongoConfigMap)

    updateMode match {
      case TazkMongoUpdateModeAction.ALLOW_INSERT => (insert(dataset, writeConfig), 0, 0)
      case TazkMongoUpdateModeAction.ALLOW_UPDATE =>
        val updateResult = update(dataset, readConfig, writeConfig)
        (updateResult._1, updateResult._2, 0)
      case TazkMongoUpdateModeAction.ALLOW_DELETE => ???
      case _ => throw new IllegalArgumentException(s"[$updateMode]不支持到类型")
    }
  }

  /**
   * 数据更新到mongo
   *
   * @param dataset    Dataset[Row]
   * @param readConfig mongo配置信息
   * @return 成功更新条数和新增条数
   */
  private def update(dataset: Dataset[Row], readConfig: ReadConfig, writeConfig: WriteConfig): (Long, Long) = {
    // mongo中现存的数据
    val mongoDS = spark.read.json(MongoSpark.load(spark.sparkContext, readConfig)
      .mapPartitions(_.map(content2JSON)).toDS)
      .alias("his_ds")
    val aliasCurDS = dataset.alias("cur_ds")

    val curCols = aliasCurDS.columns
    val hisCols = mongoDS.columns

    // 需要新增添加的数据
    val preInsert = aliasCurDS.join(mongoDS, $"$updateKey", "left semi join")
      .where(s"his_ds.$updateKey is null")
    val insertCount = insert(preInsert, writeConfig)

    // 需要更新的数据
    val updateMongoCount = spark.sparkContext.longAccumulator("UPDATE_MONGO_COUNT")
    val updateData = aliasCurDS.join(mongoDS, $"$updateKey", "inner join")
      .selectExpr(Utils.findColNams(curCols, hisCols, "cur_ds", "his_ds",
        ignoreUpdateKey.getOrElse("") ++ "his_ds._id"): _*)
    intoMongo(updateData, writeConfig, updateMongoCount, (collection, docList) => collection.bulkWrite(docList))

    (insertCount, updateMongoCount.value)
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
    intoMongo(dataset, writeConfig, insertMongoCount, (collection, docList) => collection.insertMany(docList))
    insertMongoCount.value
  }

  /**
   * 数据进入mongo
   *
   * @param dataset         Dataset[Row]
   * @param writeConfig     写入配置
   * @param longAccumulator 计数器
   * @param intoMongoFun    进入mongo的方式，insert或者update
   */
  private def intoMongo(dataset: Dataset[Row], writeConfig: WriteConfig,
                        longAccumulator: LongAccumulator, intoMongoFun: (MongoCollection[Document], JList[Document]) => Unit): Unit = {
    dataset.foreachPartition(rowPartition => {
      // 创建Mongoecotr
      val mongoConnector = MongoConnector(writeConfig.asOptions)
      // 获取dataset的所有列名,如果需要将列名进行转换，则进行转换
      val colNames = if (camelConvert) {
        dataset.columns.map(Utils.line2Hump)
      } else dataset.columns

      if (rowPartition.nonEmpty) {
        // 将Row转换成Document后，再进行批量插入
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          val docList = rowPartition.map(row => {
            val valMap: JMap[String, JObject] = (for (index <- 0 until row.size) yield {
              colNames(index) -> row.get(index).asInstanceOf[JObject]
            }).toMap.asJava
            new Document(valMap)
          }).toList.asJava

          // 添加到mongo中
          intoMongoFun(collection, docList)
          longAccumulator.add(docList.size())
        })
      }
    })
  }

  /**
   * 是否进行转换
   */
  override val camelConvertBool: Boolean = camelConvert
}
