package com.tazk.util

import java.util.Random

import com.google.gson.Gson
import com.tazk.internal.Logging

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/22
 *
 */
private[tazk] object Utils extends Logging {

  val random = new Random()

  def getTazkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrTazkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getTazkClassLoader)


  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrTazkClassLoader)
    // scalastyle:on classforname
  }

  def toJSON[T](t: T): String = {
    new Gson().toJson(t)
  }

  def parseObject[T](jsonStr: String, clazz: Class[T]): T = {
    new Gson().fromJson[T](jsonStr, clazz)
  }

}
