package com.tazk.sink

/**
 *
 * 抽象Sink，所有Sink都需要实现这个Sink
 *
 * @author zap
 * @version 1.0, 2020/05/23
 *
 */
trait TazkSink[T, R] extends Serializable {


  /**
   * 写入目标库
   *
   * @return 返回到信息，一般返回其条数
   */
  def write(t: T): R

}
