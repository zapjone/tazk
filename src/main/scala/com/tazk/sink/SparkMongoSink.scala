package com.tazk.sink

import java.util.{List => JList}

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{DeleteOneModel, Filters, UpdateOneModel, UpdateOptions}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql.TazkMapFunctions.rowToDocumentMapper
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.tazk.common.TazkCommon
import com.tazk.deploy.TazkMongoUpdateModeAction
import com.tazk.deploy.TazkMongoUpdateModeAction.TazkMongoUpdateModeAction
import com.tazk.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.bson.BsonDocument

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
                     queryOnlyColumn: Boolean = false,
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
        val updateResult = update(dataset, readConfig, writeConfig, mongoConfigMap)
        (updateResult._1, updateResult._2, 0)
      case TazkMongoUpdateModeAction.ALLOW_DELETE => delete(dataset, readConfig, writeConfig, mongoConfigMap)

      case _ => throw new IllegalArgumentException(s"[$updateMode]不支持到类型")
    }
  }

  /**
   * 需要删除到数据
   *
   * @param currentDS   Dataset[Row
   * @param readConfig  mongo读取配置信息
   * @param writeConfig mongo写入配置信息
   * @return
   */
  private def delete(currentDS: Dataset[Row],
                     readConfig: ReadConfig, writeConfig: WriteConfig,
                     mongoConfigMap: Map[String, String]): (Long, Long, Long) = {
    assert(updateKey.nonEmpty, "当更新模式时，更新key不能为空")
    val updateKeyStr = updateKey.get

    val deleteCount = spark.sparkContext.longAccumulator("DELETE_MONGO_COUNT")
    val historyCount = spark.sparkContext.longAccumulator("HISTORY_MONGO_COUNT")

    // 查询mongo历史数据
    val mongoDS = readMongoHistory(readConfig, historyCount, mongoConfigMap)

    // 检查mongo中是否有数据
    if (historyCount.value <= 0) {
      (insert(currentDS, writeConfig), 0, 0)
    } else {
      // 使用left_anti进行join，返回左边表中不存在于右边表中的数据，也就是历史表存在，当前不存在
      val deleteData = mongoDS.join(currentDS,
        mongoDS(updateKeyStr) === currentDS(updateKeyStr), "left_anti")

      // 驼峰转换名称
      val convertKeyName = if (camelConvertBool) Utils.line2Hump(updateKeyStr) else updateKeyStr

      // 批量删除
      operateMongo[BsonDocument](deleteData, writeConfig, deleteCount, (collection, docList) => {
        val delete = docList.asScala.map { doc =>
          new DeleteOneModel[BsonDocument](Filters.eq(s"$convertKeyName", doc.get(convertKeyName)))
        }
        collection.bulkWrite(delete.asJava)
      })

      // 删除多余数据后进行更新
      val updateInfo = update(currentDS, readConfig, writeConfig, mongoConfigMap)

      (updateInfo._1, updateInfo._2, deleteCount.value)
    }
  }

  /**
   * 数据更新到mongo
   *
   * @param currentDS   Dataset[Row]
   * @param readConfig  mongo配置信息
   * @param writeConfig mongo写入配置信息
   * @return 成功更新条数和新增条数
   */
  private def update(currentDS: Dataset[Row],
                     readConfig: ReadConfig, writeConfig: WriteConfig,
                     mongoConfigMap: Map[String, String]): (Long, Long) = {
    assert(updateKey.nonEmpty, "当更新模式时，更新key不能为空")
    val updateKeyStr = updateKey.get

    val histroyCount = spark.sparkContext.longAccumulator("UPDATE_HISTORY_MONGO_COUNT")

    // mongo中现存的数据
    val mongoHistoryDS = readMongoHistory(readConfig, histroyCount, mongoConfigMap)

    // 检查mongo中是否有数据
    if (histroyCount.value <= 0) {
      (insert(currentDS, writeConfig), 0)
    } else {

      // 需要新增添加的数据，left_anti返回左边表在右边表中无法匹配的数据，也就是当前存在历史不存在
      val preInsert = currentDS.join(mongoHistoryDS,
        currentDS(updateKeyStr) === mongoHistoryDS(updateKeyStr), "left_anti")
      val updateMongoCount = spark.sparkContext.longAccumulator("UPDATE_MONGO_COUNT")
      val insertCount = insert(preInsert, writeConfig)

      // 需要更新的数据
      val currentColumns = currentDS.columns
      val updateData = currentDS.join(mongoHistoryDS,
        currentDS(updateKeyStr) === mongoHistoryDS(updateKeyStr), "left_semi")
        .selectExpr(Utils.findColNams(currentColumns, ignoreUpdateKey.getOrElse("")): _*)

      // 驼峰转换名称
      val convertKeyName = if (camelConvertBool) Utils.line2Hump(updateKeyStr) else updateKeyStr

      // 批量更新
      operateMongo[BsonDocument](updateData, writeConfig, updateMongoCount, (collection, docList) => {
        val upsert = docList.asScala.map { doc =>
          val modifiers = new BsonDocument()
          modifiers.put("$set", doc)
          new UpdateOneModel[BsonDocument](Filters.eq(s"$convertKeyName", doc.get(convertKeyName)),
            modifiers, new UpdateOptions().upsert(true))
        }
        collection.bulkWrite(upsert.asJava)
      })

      (insertCount, updateMongoCount.value)
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
    operateMongo[BsonDocument](dataset, writeConfig, insertMongoCount, (collection, docList) => collection.insertMany(docList))
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
                                        longAccumulator: LongAccumulator, intoMongoFun: (MongoCollection[BsonDocument], JList[BsonDocument]) => Unit): Unit = {

    // 创建Mongoecotr
    val mongoConnector = MongoConnector(writeConfig.asOptions)

    val mapper = rowToDocumentMapper(dataset.schema, fieldName => if (camelConvertBool) Utils.line2Hump(fieldName) else fieldName)
    val documentRdd: RDD[BsonDocument] = dataset.rdd.map(row => mapper(row))

    documentRdd.foreachPartition(iter => if(iter.nonEmpty){
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[BsonDocument] =>
        iter.grouped(writeConfig.maxBatchSize).foreach(batch =>{
          intoMongoFun(collection, batch.toList.asJava)
          longAccumulator.add(batch.size)
        })
      })
    })
  }

  /**
   * 读取mongo历史数据
   *
   * @param readConfig mongo配置
   */
  private def readMongoHistory(readConfig: ReadConfig, historyCount: LongAccumulator,
                               mongoConfigMap: Map[String, String]): Dataset[Row] = {
    val mongoHistoryJSONData = if (queryOnlyColumn) {
      // 只获取需要进行关联的字段，以节约内存
      val humpUpdateKeyArray = if (camelConvert) {
        Utils.humpArray2lineArray(updateKey.get.split(","))
      } else updateKey.get.split(",")
      val schema = DataTypes.createStructType(humpUpdateKeyArray.map(DataTypes.createStructField(_, DataTypes.StringType, true)))

      // 加载mongo指定字段
      val mongoRow = spark.read.schema(schema).format("com.mongodb.spark.sql").options(mongoConfigMap).load()
      mongoRow.mapPartitions(iter => {
        iter.map { row =>
          val result = for (index <- 0 until humpUpdateKeyArray.size) yield {
            val fieldName = if (camelConvert) Utils.hump2Line(humpUpdateKeyArray(index)) else humpUpdateKeyArray(index)
            fieldName -> row.getString(index)
          }
          historyCount.add(1)
          Utils.toJSON[Map[String, AnyRef]](result.toMap)
        }
      })
    } else {
      // 直接获取全量数据
      MongoSpark.load(spark.sparkContext, readConfig)
        .mapPartitions(_.map { m =>
          historyCount.add(1)
          content2JSON(m)
        }).toDS
    }

    // 使用SparkSession解析json字符串
    spark.read.json(mongoHistoryJSONData)
  }


}
