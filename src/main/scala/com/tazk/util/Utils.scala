package com.tazk.util

import java.util.Random
import java.util.regex.Pattern

import com.tazk.internal.Logging
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.JsonMethods.{compact, render, _}
import org.json4s.{DefaultFormats, Extraction, Formats}

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

  /**
   * 根据用户名和密码来拼接mongo uri
   *
   * @param uri      uri，其中可能不含有用户名和密码
   * @param userName 用户名
   * @param password 密码
   */
  def buildMongoUri(uri: String, userName: String, password: String): String = {
    val mongoPrefix = "mongodb://"
    if (uri.startsWith(mongoPrefix)) {
      // 是否包含用户名和密码
      val withUser = if (null != userName && !uri.contains(userName)) {
        val hostIndex = uri.indexOf("@")
        val index = if (-1 == hostIndex) uri.indexOf("//") + 1 else hostIndex + 1
        s"$mongoPrefix$userName@${uri.substring(index + 1)}"
      } else uri
      if (null != password && !uri.contains(password)) {
        val index = withUser.indexOf("@")
        val prefix = withUser.substring(0, index)
        val suffix = withUser.substring(index)
        s"$prefix:$password$suffix"
      } else withUser
    } else s"$mongoPrefix$userName${if (null != password) s":$password" else ""}@$uri"
  }

  private val LINE_PATTERN = Pattern.compile("_(\\w)")

  /**
   * 下划线转驼峰
   */
  def line2Hump(lineStr: String): String = {
    val matcher = LINE_PATTERN.matcher(lineStr.toLowerCase)
    val buffer = new StringBuffer()
    while (matcher.find()) {
      matcher.appendReplacement(buffer, matcher.group(1).toUpperCase)
    }
    matcher.appendTail(buffer)
    buffer.toString
  }

  private val HUMP_PATTERN = Pattern.compile("[A-Z]")

  /**
   * 驼峰转下划线
   */
  def hump2Line(humpStr: String): String = {
    val matcher = HUMP_PATTERN.matcher(humpStr)
    val buffer = new StringBuffer()
    while (matcher.find()) {
      matcher.appendReplacement(buffer, s"_${matcher.group(0).toLowerCase()}")
    }
    matcher.appendTail(buffer)
    buffer.toString
  }

  /**
   * 查找出需要的字段信息
   *
   * @param curCol    当前列名称
   * @param hisCol    历史列名称
   * @param cur       当前别名
   * @param his       历史别名
   * @param ignoreKey 忽略的key
   * @return 需要查询的key
   */
  def findColNams(curCol: Array[String], hisCol: Array[String], cur: String, his: String, ignoreKey: String): Array[String] = {
    val ignoreArr = ignoreKey.split(",")
    if (ignoreArr.size <= 0) curCol.map(c => s"$cur.$c")
    else {
      curCol.map { colName =>
        if (ignoreArr.contains(colName) && hisCol.contains(colName)) s"$his.$colName"
        else s"$cur.$colName"
      }
    }
  }

}
