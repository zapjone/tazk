package com.tazk.util

import com.tazk.core.SparkExportArguments
import com.tazk.deploy.TazkMongoUpdateModeAction

/**
 *
 * @author zhangap 
 * @version 1.0, 2020/5/26
 *
 */
object JSONTest {

  def main(args: Array[String]): Unit = {

    val exportArgs = SparkExportArguments("testSparkName", "mongo://127.0.0.1:3710", "mongo_db",
      "mongo_table", "mongo_user", "mongo_pass", mongoQueryOnlyColumn = true, mongoCamelConvert = true,
      TazkMongoUpdateModeAction.allowInsert, Some("monso_field"), None, None,
      "hive_db", "hive_table", "", Some("date='2020-05-25'"))

    val json = Utils.toJSONWithEnum(exportArgs, TazkMongoUpdateModeAction)
    println(json)

    val exportObj = Utils.parseObjectWithEnum[SparkExportArguments](json, TazkMongoUpdateModeAction)

    println(exportObj)

  }

}
