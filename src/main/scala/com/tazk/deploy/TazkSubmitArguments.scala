package com.tazk.deploy

import com.tazk.deploy.TazkExecutionEngineAction.TazkExecutionEngineAction
import com.tazk.deploy.TazkMongoUpdateModeAction.TazkMongoUpdateModeAction
import com.tazk.deploy.TazkSubmitAction.TazkSubmitAction
import com.tazk.launcher.TazkSubmitArgumentsParser

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/22
 *
 */
private[tazk] class TazkSubmitArguments(args: List[String], env: Map[String, String] = sys.env) extends TazkSubmitArgumentsParser {

  var action: TazkSubmitAction = null
  var executionEngine: TazkExecutionEngineAction = TazkExecutionEngineAction.SPARK
  var name: String = null
  var verbose: Boolean = false
  var connect: String = null
  var username: String = null
  var password: String = null
  var jar: String = null
  var sparkHome: String = null
  var sparkMaster: String = "yarn"
  var sparkDeployMode: String = "cluster"
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var sparkPropertiesFile: String = null
  var sparkQueue: String = "default"
  var sparkDriverMemory: String = "1G"
  var sparkDriverCores: String = "1"
  var sparkExecutorMemory: String = "1G"
  var sparkExecutorCores: String = "1"
  var sparkTotalExecutorCores: String = null
  var sparkNumExecutor: String = null
  var mongoDatabase: String = null
  var mongoCollection: String = null
  var mongoReadPreference: String = "primaryPreferred"
  val mongoexternalProperties: HashMap[String, String] = new mutable.HashMap[String, String]()
  var mongoImportCondition: String = null
  var mongoImportConditionEncrypt: String = "base64"
  var mongoUpdateKey: String = null
  var mongoIgnoreUpdateKey: String = null
  var mongoUpdateMode: String = TazkMongoUpdateModeAction.allowInsert
  var mongoCamelConvert: Boolean = true
  var hiveDatabase: String = "default"
  var hiveTable: String = null
  var hiveAutoCreateTable: Boolean = false
  var hiveDeleteTableIfExists: Boolean = false
  var hiveFormat: String = null
  var hivePartitionKey: String = null
  var hivePartitionValue: String = null
  var hiveEnableDynamic: Boolean = false
  var hiveDynamicPartitionKey: String = null
  var hiveExportCondition: String = null

  // Set parameters from command line arguments
  loadEnvironmentInfo()
  try {
    if (null == args || args.isEmpty || (!Array("import", "export").contains(args.head))) {
      printUsageAndExit(1)
    } else {
      action = syncModeParse(args.head)
      parse(args.tail.asJava)
    }
  } catch {
    case e: Exception =>
      TazkSubmit.printErrorAndExit(e.getMessage)
  }

  validateArguments()


  override def toString: String = {
    s"""Parsed arguments:
       |  import/export
       |  spark-master                   $sparkMaster
       |  spark-deployMode               $sparkDeployMode
       |  spark-driver-memory            $sparkDriverMemory
       |  spark-driver-cores             $sparkDriverCores
       |  spark-executor-memory          $sparkExecutorMemory
       |  spark-total-executor-cores     $sparkTotalExecutorCores
       |  spark-num-executors            $sparkNumExecutor
       |  spark-executor-core            $sparkExecutorCores
       |  mongo-database                 $mongoDatabase
       |  mongo-collection               $mongoCollection
       |  mongo-import-condition         $mongoImportCondition
       |  mongo-import-condition-encrypt $mongoImportConditionEncrypt
       |  mongo-update-key               $mongoUpdateKey
       |  mongo-ignore-update-key        $mongoIgnoreUpdateKey
       |  mongo-update-mode              $mongoUpdateMode
       |  mongo-camel-convert            $mongoCamelConvert
       |  mongo-read-preference          $mongoReadPreference
       |  hive-database                  $hiveDatabase
       |  hive-table                     $hiveTable
       |  hive-auto-create-table         $hiveAutoCreateTable
       |  hive-delete-table-if-exists    $hiveDeleteTableIfExists
       |  hive-format                    $hiveFormat
       |  hive-partition-key             $hivePartitionKey
       |  hiev-partition-value           $hivePartitionValue
       |  hive-enable-dynamic            $hiveEnableDynamic
       |  hive-dynamic-partition-key     $hiveDynamicPartitionKey
       |  verbose                        $verbose
    """.stripMargin
  }


  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case EXECUTION_ENGINE => value match {
        case "spark" => executionEngine = TazkExecutionEngineAction.SPARK
        case _ => new IllegalArgumentException(s"[$value]当前不支持")
      }
      case NAME => name = value
      case CONNECT => connect = value
      case USER_NAME => username = value
      case PASSWORD => password = value
      case JAR_ADDR => jar = value
      case SPARK_HOME => sparkHome = value
      case SPARK_MASTER => sparkMaster = value
      case SPARK_DEPLOY_MODE => sparkDeployMode = value
      case SPARK_CONF =>
        val (confName, confValue) = TazkSubmit.parseTazkConfProperty(value)
        sparkProperties(confName) = confValue
      case SPARK_PROPS_FILE => sparkPropertiesFile = value
      case SPARK_QUEUE => sparkQueue = value
      case SPARK_DRIVER_MEM => sparkDriverMemory = value
      case SPARK_DRIVER_CORES => sparkDriverCores = value
      case SPARK_EXECUTOR_MEM => sparkExecutorMemory = value
      case SPARK_TOTAL_EXECUTOR_CORES => sparkTotalExecutorCores = value
      case SPARK_NUM_EXECUTORS => sparkNumExecutor = value
      case SPARK_EXECUTOR_CORES => sparkExecutorCores = value
      case MONGO_DATABASE => mongoDatabase = value
      case MONGO_COLLECTION => mongoCollection = value
      case MONGO_READ_PREFERENCE => mongoReadPreference = value
      case MONGO_EXTERNAL_CONF =>
        val (confName, confValue) = TazkSubmit.parseTazkConfProperty(value)
        mongoexternalProperties(confName) = confValue
      case MONGO_IMPORT_CONDITION => mongoImportCondition = value
      case MONGO_IMPORT_CONDITION_ENCRYPT_TYPE => mongoImportConditionEncrypt = value
      case MONGO_UPDATE_KEY => mongoUpdateKey = value
      case MONGO_IGNORE_UPDATE_KEY => mongoIgnoreUpdateKey = value
      case MONGO_UPDATE_MODE => mongoUpdateMode = value
      case MONGO_CAMEL_CONVERT => mongoCamelConvert = value.toBoolean
      case HIVE_DATABASE => hiveDatabase = value
      case HIVE_TABLE => hiveTable = value
      case HIVE_AUTO_CREATE_TABLE => hiveAutoCreateTable = value.toBoolean
      case HIVE_DELETE_TABLEIF_EXISTS => hiveDeleteTableIfExists = value.toBoolean
      case HIVE_FORMAT => hiveFormat = value
      case HIVE_PARTITION_KEY => hivePartitionKey = value
      case HIVE_PARTITION_VALUE => hivePartitionValue = value
      case HIVE_ENABLE_DYNAMIC => hiveEnableDynamic = value.toBoolean
      case HIVE_DYNAMIC_PARTITION_KEY => hiveDynamicPartitionKey = value
      case HIVE_EXPORT_CONDITION => hiveExportCondition = value

      case VERBOSE => verbose = true
      case VERSION => TazkSubmit.printVersionAndExit()
      case HELP => printUsageAndExit(0)

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }

    true
  }

  override protected def handleUnknown(opt: String): Boolean = {
    TazkSubmit.printErrorAndExit(s"Unrecognized option '$opt'.")
    false
  }

  /**
   * 显示输出信息
   *
   * @param exitCode     退出码
   * @param unknownParam 未知参数
   */
  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    val outStream = TazkSubmit.printStream
    if (null != unknownParam) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }

    val command = env.getOrElse("_TAZK_CMD_USAGE",
      """Usage: tazk-submit import/export ...
        |""".stripMargin)
    outStream.println(command)

    outStream.println(
      s"""
         |Options:
         |  import/export                     同步方式，导入或者导出
         |  --execution-engine                执行引擎方式，默认spark引擎执行
         |  --name                            任务执行名称
         |  --connect                         连接url地址
         |  --username                        用户名称
         |  --password                        用户密码
         |  --help                            查看帮助信息
         |
         | Spark-only:
         |  --spark-master                    Spark运行模式，本地或yarn，默认yarn
         |  --spark-deplopy-mode              Spark部署模式，默认cluster模式
         |  --spark-conf                      Spark运行时配置的参数信息
         |  --spark-properties-file           Spark配置参数文件
         |  --spark-queue                     Spark运行队列名称
         |  --spark-driver-memory             Spark运行Driver内存，默认1G
         |  --spark-driver-cores              Spark运行Driver的核心数，默认1
         |  --spark-driver-memory             Spark运行Driver内存，默认1G
         |  --spark-num-executor              Spark运行Executor个数
         |  --spark-executor-cores            Spark运行Executor核心数
         |  --spark-total-executor-cores      Spark最大运行Executor的核心数
         |
         | Mongo-only:
         |  --mongo-database                  Mongo数据库名称
         |  --mongo-collection                Mongo集合名称
         |  --mongo-conf                      Mongo格外配置
         |  --mongo-import-condition          Mongo增量导入的条件
         |  --mongo-import-condition-encrypt  Mongo增量导入的条件加密类型，
         |                                    当使用Spark on Yarn且为Cluster模式增量导入时，必须配置
         |  --mongo-update-key                Mongo导出时需进行join的key
         |  --mongo-ignore-updte-key          Mongo更新导出时需要忽略更新的key
         |                                    默认除update-key之外的所有key都需要进行更新
         |  --mongo-update-mode               Mongo导出模式：
         |                                    allowInsert:只导出增加数据
         |                                    allowUpdate:导出新增数据和更新数据
         |                                    allowDelete:导出新增数据和更新历史数据，且删除历史不需要都数据
         |  --mongo-camel-convert             是否进行驼峰命名转换，默认为true
         |
         | Hive-only:
         |  --hive-database                   hive数据库名称，默认default
         |  --hive-table                      hive表名称
         |  --hive-auto-create-table          是否自动创建hive表,默认true
         |  --hive-delete-table-if-exists     当hive存在导出表时，是否需要进行删除，默认为false
         |  --hive-format                     hive存储格式,默认textfile
         |  --hive-partition-key              当增量导入时，hiv的分区键，多个用顺序逗号分隔
         |  --hive-partition-value            当增量导入时，hive当分区键值，多个用顺序逗号分隔，
         |                                    且必须和【--hive-partition-key】个数保持一致
         |  --hive-enable-dynamic             是否开启hive动态分区，默认false
         |  --hive-dynamic-partition-key      当hive开启动态分区时的分区键，多个用顺序逗号分隔
         |
      """.stripMargin
    )

    TazkSubmit.exitFn(exitCode)

  }

  /**
   * 同步方式
   */
  private def syncModeParse(mode: String): TazkSubmitAction = mode match {
    case "import" => TazkSubmitAction.IMPORT
    case "export" => TazkSubmitAction.EXPORT
    case _ => throw new IllegalArgumentException(String.format("[%s]当前不支持的操作，目前只支持import/export", mode))
  }

  private def loadEnvironmentInfo(): Unit = {
    // 从环境变量中获取Spark安装路径
    sparkHome = env.getOrElse("SPARK_HOME", env.getOrElse("TAZK_SPARK_HOME", null))
  }


  /**
   * 验证输入参数
   */
  private def validateArguments(): Unit = {
    // 验证共同参数（导入和导出都需要的参数）
    if (null == connect || connect.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No connect set; please specify one with --connect")
    }
    // 用户名和密码可以配置在uri中
    /*if (null == username || username.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No username set; please specify one with --username")
    }
    if (null == password || password.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No password set; please specify one with --password")
    }*/
    if (null == mongoDatabase || mongoDatabase.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No mongo database set; please specify one with --mongo-database")
    }
    if (null == mongoCollection || mongoCollection.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No mongo collection set; please specify one with --mongo-collection")
    }
    if (null == hiveDatabase || mongoDatabase.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No hive database set; please specify one with --hive-database")
    }
    if (null == hiveTable || hiveTable.trim.length <= 0) {
      TazkSubmit.printErrorAndExit("No hive table set; please specify one with --hive-table")
    }
    if (executionEngine == TazkExecutionEngineAction.SPARK && null == sparkHome) {
      throw new IllegalArgumentException("当执行引擎为Spark时，SPARK_HOME必须配置")
    }
    if (null == name) {
      name = s"${mongoDatabase}_$mongoCollection"
    }

    // 验证mongo读取方式
    if (null != mongoReadPreference
      && "primary" != mongoReadPreference
      && "primaryPreferred" != mongoReadPreference
      && "secondary" != mongoReadPreference
      && "secondaryPreferred" != mongoReadPreference
      && "nearest" != mongoReadPreference) {
      throw new IllegalArgumentException(s"Mongo ReadPreference输入错误:$mongoReadPreference")
    }

    // 根据同步方式验证参数
    action match {
      case TazkSubmitAction.IMPORT => validateImportArguments()
      case TazkSubmitAction.EXPORT => validateExportArguments()
    }
  }

  /**
   * 验证导入相关参数
   */
  private def validateImportArguments(): Unit = {
    // 按引擎验证参数
    executionEngine match {
      case TazkExecutionEngineAction.SPARK => validateSparkImportArguments()
    }
  }

  /**
   * 验证导出相关参数
   */
  private def validateExportArguments(): Unit = {
    // 按引擎验证参数
    executionEngine match {
      case TazkExecutionEngineAction.SPARK => validateSparkExportArguments()
    }
  }

  /**
   * 验证spark引擎相关导入参数
   */
  private def validateSparkImportArguments(): Unit = {
    validateSparkImportantArguments()
  }

  /**
   * 验证spark引擎相关导出参数
   */
  private def validateSparkExportArguments(): Unit = {
    validateSparkImportantArguments()
  }

  private def validateSparkImportantArguments(): Unit = {
    sparkMaster match {
      case "yarn" if !Array("cluster", "client").contains(sparkDeployMode) =>
        throw new IllegalArgumentException(String.format("[%s]不支持,yarn模式只支持cluster/client", sparkDeployMode))
      case "yarn" if ("cluster" == sparkDeployMode && null != mongoImportCondition
        && (null == mongoImportConditionEncrypt || "base64" != mongoImportConditionEncrypt)) =>
        throw new IllegalArgumentException("Spark提交模式为yarn时，进行条件导入，需要将条件信息进行加密，如base64加密")
      case _ =>
    }
  }

}
