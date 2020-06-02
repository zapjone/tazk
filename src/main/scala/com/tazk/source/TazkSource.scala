package com.tazk.source

/**
 *
 * 抽象都source，所有source都需要实现这个source
 *
 * @author zap
 * @version 1.0, 2020/05/23
 *
 */
trait TazkSource[T] extends Serializable {

  /**
   * 读取数据
   */
  def read(): T

}
