package com.tazk.deploy

import java.util.concurrent.CountDownLatch

import com.tazk.internal.Logging
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

/**
 * Spark提交作业
 *
 * @author zhangap 
 * @version 1.0, 2020/5/25
 *
 */
private[tazk] class TazkSparkDepoly(clazz: String,
                                    appInputArgs: String,
                                    appArgs: TazkSubmitArguments) extends TazkDepolyJob with Logging {
  /**
   * 提交任务
   */
  override def runForWait(): Unit = {
    // 使用SparkLauncher来启动程序
    var launcher = new SparkLauncher()
      .setSparkHome(appArgs.sparkHome)
      .setMaster(appArgs.sparkMaster)
      .setDeployMode(appArgs.sparkDeployMode)
      .setAppName(appArgs.name)
      .setMainClass(clazz)
      .setAppResource(appArgs.jar)
      .addAppArgs(appInputArgs)
      .setConf("spark.yarn.queue", appArgs.sparkQueue)
      .setConf("spark.driver.memory", appArgs.sparkDriverMemory)
      .setConf("spark.driver.cores", appArgs.sparkDriverCores)
      .setConf("spark.executor.instances", appArgs.sparkNumExecutor)
      .setConf("spark.executor.memory", appArgs.sparkExecutorMemory)
      .setConf("spark.executor.cores", appArgs.sparkExecutorCores)
      .setConf("spark.default.parallelism", "200")
      .setConf("spark.sql.shuffle.partitions", "200")

    if (null != appArgs.sparkTotalExecutorCores) {
      launcher = launcher.setConf("spark.cores.max", appArgs.sparkTotalExecutorCores)
    }

    // 添加spark启动conf信息
    appArgs.sparkProperties.foreach(entry => {
      launcher = launcher.setConf(entry._1, entry._2)
    })

    // 开始启动Spark程序
    val countDownLatch = new CountDownLatch(1)
    launcher.setVerbose(true).startApplication(new SparkAppHandle.Listener {
      override def stateChanged(handle: SparkAppHandle): Unit = {
        val state = handle.getState
        if (state.isFinal) {
          println(String.format("[%s]作业执行完成", handle.getAppId))
          log.info(String.format("[%s]作业执行完成", handle.getAppId))
          Thread.sleep(3000)
          countDownLatch.countDown()
        } else {
          println(state)
        }
      }

      override def infoChanged(handle: SparkAppHandle): Unit = {
        // nothing
      }
    })

    countDownLatch.await()
  }

}
