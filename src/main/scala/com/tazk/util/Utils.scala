package com.tazk.util

import java.util.Random

import com.tazk.internal.Logging
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.jackson.JsonMethods._

import scala.reflect.ClassTag

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


  /**
   * 将对象转换成JSON字符串
   *
   * @param es 枚举类型
   */
  def toJSONWithEnum[T, E <: Enumeration : ClassTag](t: T, es: E*): String = {
    implicit val formats: Formats = DefaultFormats ++ es.map(en => new EnumNameSerializer(en))
    compact(render(Extraction.decompose(t)))
  }

  /**
   * 将对象转换成JSON字符串
   */
  def toJSON[T: Manifest](t: T): String = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    compact(render(Extraction.decompose(t)))
  }

  /**
   * 将字符串转换成成对象
   *
   * @param jsonStr json字符串
   * @param es      枚举类
   * @tparam T 对象类型
   */
  def parseObjectWithEnum[T: Manifest](jsonStr: String, es: Enumeration*): T = {
    implicit val formats: Formats = DefaultFormats ++ es.map(en => new EnumNameSerializer(en))
    parse(jsonStr).extract[T]
  }

  /**
   * 将字符串转换成成对象
   *
   * @param jsonStr json字符串
   * @tparam T 对象类型
   */
  def parseObject[T: Manifest](jsonStr: String): T = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(jsonStr).extract[T]
  }


}
