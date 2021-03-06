package com.tazk.deploy

import java.io.PrintStream

import com.tazk.core.TazkFactory
import com.tazk.internal.Logging
import com.tazk.util.CommandLineUtils
import com.tazk.{deploy, _}

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

private[tazk] object TazkExecutionEngineAction extends Enumeration {
  type TazkExecutionEngineAction = Value
  /**
   * 使用spark引擎执行
   */
  val SPARK: deploy.TazkExecutionEngineAction.Value = Value("spark")
}

/**
 * mongo更新模式
 */
private[tazk] object TazkMongoUpdateModeAction extends Enumeration {
  val allowInsert = "allowInsert"
  val allowUpdate = "allowUpdate"
  val allowDelete = "allowDelete"
  type TazkMongoUpdateModeAction = Value
  val ALLOW_INSERT: deploy.TazkMongoUpdateModeAction.Value = Value(allowInsert)
  val ALLOW_UPDATE: deploy.TazkMongoUpdateModeAction.Value = Value(allowUpdate)
  val ALLOW_DELETE: deploy.TazkMongoUpdateModeAction.Value = Value(allowDelete)

  /**
   * 以字符串查找更新模式
   */
  def findOf(mode: String): TazkMongoUpdateModeAction = mode match {
    case TazkMongoUpdateModeAction.allowInsert => ALLOW_INSERT
    case TazkMongoUpdateModeAction.allowUpdate => ALLOW_UPDATE
    case TazkMongoUpdateModeAction.allowDelete => ALLOW_DELETE
    case _ => throw new RuntimeException(s"未知的[$mode]导出模式")
  }

}

/**
 * hive sink的表结构
 */
private[tazk] object TazkHiveTableModeAction extends Enumeration {
  val json = "json"
  val struct = "struct"
  type TazkHiveTableModeAction = Value
  val JSON: TazkHiveTableModeAction.Value = TazkHiveTableModeAction.Value(json)
  val STRUCT: TazkHiveTableModeAction.Value = TazkHiveTableModeAction.Value(struct)
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

    // 解析输入参数
    val appArgs = new TazkSubmitArguments(args.toList)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }

    // 输出欢迎信息
    printWelcomeInfo(System.out)

    submitAction(appArgs)
  }

  /**
   * 提交任务进行导出和导出
   */
  private def submitAction(appArgs: TazkSubmitArguments): Unit = {
    val submitForEngine = appArgs.action match {
      case TazkSubmitAction.IMPORT => TazkFactory.importAction(appArgs, appArgs.executionEngine)
      case TazkSubmitAction.EXPORT => TazkFactory.exportAction(appArgs, appArgs.executionEngine)
    }
    submitForEngine.runForWait()
  }

}
