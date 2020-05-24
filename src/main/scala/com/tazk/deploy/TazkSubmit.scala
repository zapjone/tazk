package com.tazk.deploy

import com.tazk.{deploy, _}
import com.tazk.internal.Logging
import com.tazk.util.CommandLineUtils

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

private[deploy] object TazkExecutionEngingAction extends Enumeration {
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
    printStream.println(
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
    printStream.println("Branch %s".format(TAZK_BRANCH))
    printStream.println("Compiled by user %s on %s".format(TAZK_BUILD_USER, TAZK_BUILD_DATE))
    printStream.println("Revision %s".format(TAZK_REVISION))
    printStream.println("Url %s".format(TAZK_REPO_URL))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }

  override def main(args: Array[String]): Unit = {

    val uninitLog = initializeLogIfNecessary(true, silent = true)

    // 解析输入参数
    val appArgs = new TazkSubmitArguments(args.toList)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }

    appArgs.action match {
      case TazkSubmitAction.IMPORT => importAction(appArgs, uninitLog)
      case TazkSubmitAction.EXPORT => exportAction(appArgs, uninitLog)
    }
  }

  /**
   * 导入
   */
  private def importAction(appArgs: TazkSubmitArguments, uninitLog: Boolean): Unit = {
  }

  /**
   * 导出
   */
  private def exportAction(appArgs: TazkSubmitArguments, uninitLog: Boolean): Unit = {
  }


}
