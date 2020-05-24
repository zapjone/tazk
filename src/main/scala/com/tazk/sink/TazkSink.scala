package com.tazk.sink

/**
 *
 * 抽象Sink，所有Sink都需要实现这个Sink
 *
 * @author zap
 * @version 1.0, 2020/05/23
 *
 */
trait TazkSink[T] {


  /**
   * 写入目标库
   */
  def write(t: T): Unit

}
