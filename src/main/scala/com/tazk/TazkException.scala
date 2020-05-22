package com.tazk

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/22
 *
 */

class TazkException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}

private[spark] case class SparkUserAppException(exitCode: Int)
  extends TazkException(s"User application exited with $exitCode")
