#!/bin/bash
#
# 导入数据样例
# mongo-import-condition和mongo-import-condition-encrypt属于增量导入的条件;
# 因spark on yarn 的cluster模式对于json的参数支持有问题，所以需要将参数继续加密并指定加密类型，当前只支持base64
#

increment_mongo=`base64 <<< "{'\$match':{mongo_field:{\$regex: '2020-01-01+'}}}"`

tazk-submit import \
--connect "mongodb://test:test@127.0.0.1:1370/admin" \
--database "test_databse" \
--collection "test_collection" \
--mongo-camel-convert true \
--mongo-import-condition "${increment_mongo}" \
--mongo-import-condition-encrypt "base64" \
--hive-database "default" \
--hive-table "test_table" \
--hive-format "orc" \
--execution-engine "spark" \
--spark-master yarn \
--spark-deploy-mode cluster \
--spark-queue "default" \
--spark-driver-memory "2G" \
--spark-driver-cores 2 \
--spark-executor-memory "2G" \
--spark-num-executors 20 \
--spark-executor-cores 2


