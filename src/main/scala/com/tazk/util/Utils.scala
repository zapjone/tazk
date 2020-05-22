package com.tazk.util

import java.util.Random

import com.tazk.internal.Logging

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/22
 *
 */
private[tazk] object Utils extends Logging{

  val random = new Random()

  def getTazkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrTazkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getTazkClassLoader)


  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrTazkClassLoader)
    // scalastyle:on classforname
  }

}
