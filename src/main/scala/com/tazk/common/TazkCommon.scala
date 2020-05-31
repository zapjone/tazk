package com.tazk.common

import com.tazk.util.Utils
import org.bson.Document

/**
 *
 * @author zap
 * @version 1.0, 2020/05/31
 *
 */
trait TazkCommon {
  /**
   * 是否进行转换
   */
  val camelConvertBool: Boolean

  /**
   * 将document中的列名驼峰命名转换成下划线
   */
  def content2JSON(document: Document): String = {
    import scala.collection.JavaConverters._
    if (camelConvertBool) {
      val multiSet = for (entry <- document.keySet().asScala) yield {
        Utils.hump2Line(entry) -> document.get(entry)
      }
      Utils.toJSON(multiSet.toMap)
    } else {
      document.toJson()
    }
  }

}
