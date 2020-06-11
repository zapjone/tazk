package com.tazk.sink

import java.lang.{Object => JObject}
import java.util.{List => JList, Map => JMap}

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{DeleteOneModel, Filters, UpdateOneModel, UpdateOptions}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.tazk.common.TazkCommon
import com.tazk.deploy.TazkMongoUpdateModeAction
import com.tazk.deploy.TazkMongoUpdateModeAction.TazkMongoUpdateModeAction
import com.tazk.util.Utils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.bson.Document

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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
                     otherConf: Option[Map[String, String]] = None,
                     updateMode: TazkMongoUpdateModeAction,
                     updateKey: Option[String],
                     ignoreUpdateKey: Option[String],
                     camelConvert: Boolean)
  extends TazkSink[Dataset[Row], (Long, Long, Long)] with TazkCommon {

  import spark.implicits._

  /**
   * 是否进行转换
   */
  override val camelConvertBool: Boolean = camelConvert

  /**
   * 写入目标库
   *
   * @return 返回写入成功条数
   */
  override def write(dataset: Dataset[Row]): (Long, Long, Long) = {

    // mongo配置信息
    val mongoOtherConfMap = if (otherConf.nonEmpty) otherConf.get else Map()
    val mongoConfigMap = Map(
      "uri" -> Utils.buildMongoUri(uri, username, password),
      "database" -> database,
      "collection" -> collection
    ) ++ mongoOtherConfMap
    val readConfig: ReadConfig = ReadConfig(mongoConfigMap)
    val writeConfig: WriteConfig = WriteConfig(mongoConfigMap)

    updateMode match {
      case TazkMongoUpdateModeAction.ALLOW_INSERT => (insert(dataset, writeConfig), 0, 0)
      case TazkMongoUpdateModeAction.ALLOW_UPDATE =>
        val updateResult = update(dataset, readConfig, writeConfig)
        (updateResult._1, updateResult._2, 0)
      case TazkMongoUpdateModeAction.ALLOW_DELETE => delete(dataset, readConfig, writeConfig)

      case _ => throw new IllegalArgumentException(s"[$updateMode]不支持到类型")
    }
  }

  /**
   * 需要删除到数据
   *
   * @param dataset     Dataset[Row
   * @param readConfig  mongo读取配置信息
   * @param writeConfig mongo写入配置信息
   * @return
   */
  private def delete(dataset: Dataset[Row], readConfig: ReadConfig, writeConfig: WriteConfig): (Long, Long, Long) = {
    val updateInfo = update(dataset, readConfig, writeConfig)
    val deleteCount = spark.sparkContext.longAccumulator("DELETE_MONGO_COUNT")

    val deleteAlias = "his_del_ds"
    val mongoDS = readMongoHistory(readConfig, deleteAlias)
    val hisCols = mongoDS.columns
    val deleteData = dataset.alias("cur_ds").join(mongoDS, $"$updateKey", "right join")
      .where(s"cur_ds.$updateKey is null")
      .selectExpr(hisCols.map(c => s"$deleteAlias.$c"): _*)

    // 批量删除
    operateMongo[Document](deleteData, writeConfig, deleteCount, (collection, docList) => {
      val delete = docList.asScala.map { doc =>
        new DeleteOneModel[Document](Filters.eq(s"$updateKey", doc.get(updateKey)))
      }
      collection.bulkWrite(delete.asJava)
    })

    (updateInfo._1, updateInfo._2, deleteCount.value)
  }

  /**
   * 数据更新到mongo
   *
   * @param dataset     Dataset[Row]
   * @param readConfig  mongo配置信息
   * @param writeConfig mongo写入配置信息
   * @return 成功更新条数和新增条数
   */
  private def update(dataset: Dataset[Row], readConfig: ReadConfig, writeConfig: WriteConfig): (Long, Long) = {
    // mongo中现存的数据
    val updateAlias = "his_up_ds"
    val mongoDS = readMongoHistory(readConfig, updateAlias)
    val aliasCurDS = dataset.alias("cur_ds")

    val curCols = aliasCurDS.columns
    val hisCols = mongoDS.columns

    // 需要新增添加的数据
    val preInsert = aliasCurDS.join(mongoDS, $"$updateKey", "left semi join")
      .where(s"$updateAlias.$updateKey is null")
    val updateMongoCount = spark.sparkContext.longAccumulator("UPDATE_MONGO_COUNT")
    val insertCount = insert(preInsert, writeConfig)

    // 需要更新的数据
    val updateData = aliasCurDS.join(mongoDS, $"$updateKey", "inner join")
      .selectExpr(Utils.findColNams(curCols, hisCols, "cur_ds", updateAlias,
        ignoreUpdateKey.getOrElse("") ++ s"$updateAlias._id"): _*)
    // 批量更新
    operateMongo[Document](updateData, writeConfig, updateMongoCount, (collection, docList) => {
      val upsert = docList.asScala.map { doc =>
        val modifiers = new Document()
        modifiers.put("$set", doc)
        new UpdateOneModel[Document](Filters.eq(s"$updateKey", doc.get(updateKey)),
          modifiers, new UpdateOptions().upsert(true))
      }
      collection.bulkWrite(upsert.asJava)
    })

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
    operateMongo[Document](dataset, writeConfig, insertMongoCount, (collection, docList) => collection.insertMany(docList))
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
  private def operateMongo[D: ClassTag](dataset: Dataset[Row], writeConfig: WriteConfig,
                                        longAccumulator: LongAccumulator, intoMongoFun: (MongoCollection[D], JList[Document]) => Unit): Unit = {
    dataset.foreachPartition(rowPartition => {
      // 创建Mongoecotr
      val mongoConnector = MongoConnector(writeConfig.asOptions)
      // 获取dataset的所有列名,如果需要将列名进行转换，则进行转换
      val colNames = if (camelConvert) {
        dataset.columns.map(Utils.line2Hump)
      } else dataset.columns

      if (rowPartition.nonEmpty) {
        // 将Row转换成Document后，再进行批量插入
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
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
   * 读取mongo历史数据
   *
   * @param readConfig mongo配置
   */
  private def readMongoHistory(readConfig: ReadConfig, aliasName: String): Dataset[Row] = {
    spark.read.json(MongoSpark.load(spark.sparkContext, readConfig)
      .mapPartitions(_.map(content2JSON)).toDS)
      .alias(aliasName)
  }
}
