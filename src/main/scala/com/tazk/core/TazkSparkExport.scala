package com.tazk.core

import com.tazk.deploy.TazkMongoUpdateModeAction
import com.tazk.internal.Logging
import com.tazk.util.Utils

/**
 *
 * 使用Spark将hive根据模式将数据导出到mongo
 *
 * @author zhangap 
 * @version 1.0, 2020/5/25
 *
 */
object TazkSparkExport extends TazkSparkCore with Logging {

  /**
   * 导出操作
   */
  override def main(args: Array[String]): Unit = {
    // 解析传递过来参数
    val arguments = Utils.parseObjectWithEnum[SparkExportArguments](checkInputArgs(args),
      TazkMongoUpdateModeAction)
    val spark = getOrCreateSparkSession(arguments.name)

    // 执行导出操作


    spark.stop()
  }


}
