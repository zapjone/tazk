package com.tazk.deploy

import com.tazk._
import com.tazk.internal.Logging
import com.tazk.util.CommandLineUtils

import scala.util.Properties

/**
 * hether to submit
 */
private[deploy] object TazkSubmitAction extends Enumeration {
  type TazkSubmitAction = Value
  /**
   * 提交任务
   */
  val SUBMIT = Value
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

    val appArgs = new TazkSubmitArguments(args)
  }
}
