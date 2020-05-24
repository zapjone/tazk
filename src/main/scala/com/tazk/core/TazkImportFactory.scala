package com.tazk.core

import com.tazk.deploy.{TazkExecutionEngingAction, TazkSubmitArguments}
import com.tazk.deploy.TazkExecutionEngingAction.TazkExecutionEngingAction
import com.tazk.util.Utils
import org.apache.commons.codec.binary.Base64

/**
 *
 * @author zap
 * @version 1.0, 2020/05/24
 *
 */
object TazkImportFactory {

  def sparkImportArgs(tazkArgs: TazkSubmitArguments): SparkImportArguments = {
    SparkImportArguments(tazkArgs.name, tazkArgs.connect, tazkArgs.mongoDatabase, tazkArgs.mongoCollection,
      tazkArgs.username, tazkArgs.password, Option(tazkArgs.mongoImportCondition),
      tazkArgs.mongoImportConditionEncrypt, tazkArgs.hiveTable, tazkArgs.hivePartitionKey,
      tazkArgs.hivePartitionValue, tazkArgs.hiveDatabase, tazkArgs.hiveFormat,
      tazkArgs.mongoCamelConvert, tazkArgs.mongoexternalProperties.toMap, tazkArgs.hiveDeleteTableIfExists,
      tazkArgs.hiveEnableDynamic, Option(tazkArgs.hiveDynamicPartitionKey))
  }

  /**
   * 导入操作
   */
  def importAction(tazkArgs: TazkSubmitArguments,
                   engine: TazkExecutionEngingAction): (String, String) = engine match {
    case TazkExecutionEngingAction.SPARK =>
      // 将参数进行格式化后传入到程序中
      val importArgs = Utils.toJSON[SparkImportArguments](sparkImportArgs(tazkArgs))
      (TazkSparkImport.getClass.getName, Base64.encodeBase64String(importArgs.getBytes()))

    case _ => throw new IllegalArgumentException("目前只支spark引擎")
  }

}
