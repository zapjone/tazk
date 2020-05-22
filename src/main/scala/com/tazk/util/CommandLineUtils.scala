package com.tazk.util

import java.io.PrintStream

import com.tazk.TazkException

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/22
 *
 */
trait CommandLineUtils {

  // Exposed for testing
  private[tazk] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)

  private[tazk] var printStream: PrintStream = System.err

  private[tazk] def printWarning(str: String): Unit = printStream.println("Warning: " + str)

  private[tazk] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }

  // scalastyle:on println

  private[tazk] def parseSparkConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toSeq match {
      case Seq(k, v) => (k, v)
      case _ => printErrorAndExit(s"Tazk config without '=': $pair")
        throw new TazkException(s"Tazk config without '=': $pair")
    }
  }

  def main(args: Array[String]): Unit

}
