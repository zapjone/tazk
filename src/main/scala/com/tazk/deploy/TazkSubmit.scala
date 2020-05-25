package com.tazk.deploy

import java.io.PrintStream
import java.util.concurrent.CountDownLatch

import com.tazk.core.{SparkImportArguments, TazkImportFactory}
import com.tazk.internal.Logging
import com.tazk.util.CommandLineUtils
import com.tazk.{deploy, _}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.util.Properties

/**
 * hether to submit
 */
private[deploy] object TazkSubmitAction extends Enumeration {
  type TazkSubmitAction = Value
  /**
   * 导入
   */
  val IMPORT: deploy.TazkSubmitAction.Value = Value("import")
  /**
   * 导出
   */
  val EXPORT: deploy.TazkSubmitAction.Value = Value("export")
}

private[tazk] object TazkExecutionEngingAction extends Enumeration {
  type TazkExecutionEngingAction = Value
  /**
   * 使用spark引擎执行
   */
  val SPARK: deploy.TazkExecutionEngingAction.Value = Value("spark")
}

/**
 * mongo更新模式
 */
private[deploy] object TazkMongoUpdateModeAction extends Enumeration {
  val allowInsert = "allowInsert"
  val allowUpdate = "allowUpdate"
  val allowDelete = "allDelete"
  type TazkMongoUpdateModeAction = Value
  val ALLOW_INSERT: deploy.TazkMongoUpdateModeAction.Value = Value(allowInsert)
  val ALLOW_UPDATE: deploy.TazkMongoUpdateModeAction.Value = Value(allowUpdate)
  val ALLOW_DELETE: deploy.TazkMongoUpdateModeAction.Value = Value(allowDelete)
}


/**
 * Main gateway of launching a Tazk application.
 */
object TazkSubmit extends CommandLineUtils with Logging {


  private[tazk] def printVersionAndExit(): Unit = {
    printWelcomeInfo(printStream)
    printStream.println("Branch %s".format(TAZK_BRANCH))
    printStream.println("Compiled by user %s on %s".format(TAZK_BUILD_USER, TAZK_BUILD_DATE))
    printStream.println("Revision %s".format(TAZK_REVISION))
    printStream.println("Url %s".format(TAZK_REPO_URL))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }

  /**
   * 输出欢迎信息
   */
  private def printWelcomeInfo(out: PrintStream): Unit = {
    out.println(
      """Welcome to
          _________  ________  ________  ___  __
      |\___   ___\\   __  \|\_____  \|\  \|\  \
      \|___ \  \_\ \  \|\  \\|___/  /\ \  \/  /|_
           \ \  \ \ \   __  \   /  / /\ \   ___  \
            \ \  \ \ \  \ \  \ /  /_/__\ \  \\ \  \
             \ \__\ \ \__\ \__\\________\ \__\\ \__\     version %s
              \|__|  \|__|\|__|\|_______|\|__| \|__|

                        """.format(TAZK_VERSION))
    printStream.println("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
  }

  override def main(args: Array[String]): Unit = {

    val uninitLog = initializeLogIfNecessary(isInterpreter = true, silent = true)

    // 解析输入参数
    val appArgs = new TazkSubmitArguments(args.toList)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }

    // 输出欢迎信息
    printWelcomeInfo(System.out)

    appArgs.action match {
      case TazkSubmitAction.IMPORT => importAction(appArgs, uninitLog)
      case TazkSubmitAction.EXPORT => exportAction(appArgs, uninitLog)
    }
  }

  /**
   * 导入
   */
  private def importAction(appArgs: TazkSubmitArguments, uninitLog: Boolean): Unit = {
    val (className, appInputArgs) = TazkImportFactory.importAction(appArgs, appArgs.executionEngine)
    // 使用SparkLauncher来启动程序
    var launcher = new SparkLauncher()
      .setSparkHome(appArgs.sparkHome)
      .setMaster(appArgs.sparkMaster)
      .setDeployMode(appArgs.sparkDeployMode)
      .setAppName(appArgs.name)
      .setMainClass(className)
      .setAppResource(appArgs.jar)
      .addAppArgs(appInputArgs)
      .setConf("spark.yarn.queue", appArgs.sparkQueue)
      .setConf("spark.driver.memory", appArgs.sparkDriverMemory)
      .setConf("spark.driver.cores", appArgs.sparkDriverCores)
      .setConf("spark.num.executors", appArgs.sparkNumExecutor)
      .setConf("spark.executor.memory", appArgs.sparkExecutorMemory)
      .setConf("spark.executor.cores", appArgs.sparkExecutorCores)
      .setConf("spark.cores.max", appArgs.sparkTotalExecutorCores)

    // 添加spark启动conf信息
    appArgs.sparkProperties.foreach(entry => {
      launcher = launcher.setConf(entry._1, entry._2)
    })

    // 开始启动Spark程序
    val countDownLatch = new CountDownLatch(1)
    launcher.setVerbose(true).startApplication(new SparkAppHandle.Listener {
      override def stateChanged(handle: SparkAppHandle): Unit = {
        val state = handle.getState
        if (state.isFinal) {
          log.info(String.format("[%s]作业执行完成", handle.getAppId))
          Thread.sleep(3000)
          countDownLatch.countDown()
        } else {
          println(state)
        }
      }

      override def infoChanged(handle: SparkAppHandle): Unit = {
        // nothing
      }
    })

    countDownLatch.await()
  }

  /**
   * 导出
   */
  private def exportAction(appArgs: TazkSubmitArguments, uninitLog: Boolean): Unit = {
  }

}
