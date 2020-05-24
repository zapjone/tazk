package com.tazk.core

import org.apache.spark.sql.SparkSession

/**
 * 导入参数
 *
 * @param name                       Spark程序启动名字
 * @param mongoUri                   mongo连接uri
 * @param mongoDatabase              mongo数据库名称
 * @param mongoCcollection           mongo集合名称
 * @param mongoUserName              mongo用户，如果uri中已包含，可以忽略
 * @param mongoPassword              mongo密码，如果uri中已包含，可以忽略
 * @param mongoCondition             mongo导入条件
 * @param mongoConditionEncrypt      当使用spark on yarn，且为cluster模式时，必须进行加密操作
 * @param hiveTable                  hive表名称
 * @param hivePartitionKey           hive分区key
 * @param hivePartitionValue         hive分区value
 * @param hiveDatabase               hive数据库，默认default
 * @param hiveFormat                 hive存储格式，默认text
 * @param mongoCamelConvert          是否将mongo字段当驼峰命名转化为下划线，默认为true
 * @param mongoOtherConf             mongo其他配置
 * @param hiveDeleteTableIfExists    如果hive表存在，是否将表进行删除，默认不删除
 * @param hiveEnableDynamicPartition 是否启动hive动态分区
 * @param hiveDynamicPartitionKeys   启动动态分区时，动态分区当列名，多个用逗号分隔
 */
case class SparkImportArguments(name: String,
                                mongoUri: String,
                                mongoDatabase: String,
                                mongoCcollection: String,
                                mongoUserName: String,
                                mongoPassword: String,
                                mongoCondition: Option[String],
                                mongoConditionEncrypt: String,
                                hiveTable: String,
                                hivePartitionKey: String,
                                hivePartitionValue: String,
                                hiveDatabase: String = "default",
                                hiveFormat: String = "text",
                                mongoCamelConvert: Boolean = true,
                                mongoOtherConf: Map[String, String] = Map(),
                                hiveDeleteTableIfExists: Boolean = false,
                                hiveEnableDynamicPartition: Boolean = false,
                                hiveDynamicPartitionKeys: Option[String] = None)

case class SparkOutputArguments()