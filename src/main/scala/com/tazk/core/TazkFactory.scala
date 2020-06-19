package com.tazk.core

import com.tazk.deploy.TazkExecutionEngineAction.TazkExecutionEngineAction
import com.tazk.deploy.{TazkDepolyJob, TazkExecutionEngineAction, TazkSparkDepoly, TazkSubmitArguments}
import com.tazk.util.Utils
import org.apache.commons.codec.binary.Base64

/**
 * 更加执行引擎选择不同的提交方式来同步数据
 *
 * @author zap
 * @version 1.0, 2020/05/24
 *
 */
private[tazk] object TazkFactory {

  private object ImportArgs {

    /**
     * 构建导入参数信息
     */
    def sparkImportArgs(tazkArgs: TazkSubmitArguments): SparkImportArguments = {
      SparkImportArguments(tazkArgs.name, tazkArgs.connect, tazkArgs.mongoDatabase, tazkArgs.mongoCollection,
        tazkArgs.username, tazkArgs.password, Option(tazkArgs.mongoImportCondition),
        tazkArgs.mongoImportConditionEncrypt, tazkArgs.hiveTable, tazkArgs.hivePartitionKey,
        tazkArgs.hivePartitionValue, tazkArgs.hiveDatabase, tazkArgs.hiveFormat, tazkArgs.hiveTableMode,
        tazkArgs.mongoCamelConvert, tazkArgs.mongoReadPreference,
        Option(tazkArgs.mongoexternalProperties.toMap), tazkArgs.hiveDeleteTableIfExists,
        tazkArgs.hiveEnableDynamic, Option(tazkArgs.hiveDynamicPartitionKey))
    }

  }

  private object ExportArgs {

    /**
     * 构建导出参数信息
     */
    def sparkExportArgs(tazkArgs: TazkSubmitArguments): SparkExportArguments = {
      SparkExportArguments(tazkArgs.name, tazkArgs.connect, tazkArgs.mongoDatabase, tazkArgs.mongoCollection,
        tazkArgs.username, tazkArgs.password, tazkArgs.mongoQueryOnlyColumn, tazkArgs.mongoCamelConvert, tazkArgs.mongoUpdateMode,
        Option(tazkArgs.mongoUpdateKey), Option(tazkArgs.mongoIgnoreUpdateKey),
        Option(tazkArgs.mongoexternalProperties.toMap), tazkArgs.mongoProxyEnable, tazkArgs.mongoProxyDatabase, tazkArgs.mongoProxyTable,
        tazkArgs.hiveDatabase, tazkArgs.hiveTable, tazkArgs.hiveIgnoreExportKey, Option(tazkArgs.hiveExportCondition))
    }

  }


  /**
   * 导入操作
   */
  def importAction(tazkArgs: TazkSubmitArguments,
                   engine: TazkExecutionEngineAction): TazkDepolyJob = engine match {
    case TazkExecutionEngineAction.SPARK =>
      // 将参数进行格式化后传入到程序中
      val importArgs = Utils.toJSON(ImportArgs.sparkImportArgs(tazkArgs))
      new TazkSparkDepoly("com.tazk.core.TazkSparkImport", Base64.encodeBase64String(importArgs.getBytes()), tazkArgs)

    case _ => throw new IllegalArgumentException("不支持的导入执行引擎")
  }

  /**
   * 导出操作
   */
  def exportAction(tazkArgs: TazkSubmitArguments,
                   engine: TazkExecutionEngineAction): TazkDepolyJob = engine match {
    case TazkExecutionEngineAction.SPARK =>
      val exportArgs = Utils.toJSON(ExportArgs.sparkExportArgs(tazkArgs))
      new TazkSparkDepoly("com.tazk.core.TazkSparkExport", Base64.encodeBase64String(exportArgs.getBytes()), tazkArgs)
    case _ => throw new IllegalArgumentException("不支持的导出执行引擎")
  }

}
