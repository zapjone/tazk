package com.tazk.deploy

/**
 *
 * 导入和导出都需要实现这个特质
 *
 * @author zhangap 
 * @version 1.0, 2020/5/25
 *
 */
private[tazk] trait TazkDepolyJob {

  /**
   * 执行任务并等待结束
   */
  def runForWait()

}
