package com.tazk.sink

import com.tazk.common.TazkCommon
import com.tazk.deploy.TazkMongoUpdateModeAction.TazkMongoUpdateModeAction
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Mongo代理，减少mongo压力
 *
 * @author zhangap 
 * @version 1.0, 2020/6/19
 *
 */
class SparkMongoProxySink(spark: SparkSession,
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
                          camelConvert: Boolean,
                          proxyDatabase: String,
                          proxyTable: String)
  extends TazkSink[Dataset[Row], (Long, Long, Long)] with TazkCommon {

  /**
   * 写入目标库
   *
   * @return 返回到信息，一般返回其条数
   */
  override def write(t: Dataset[Row]): (Long, Long, Long) = ???


  /**
   * 是否进行转换
   */
  override val camelConvertBool: Boolean = camelConvert
}
