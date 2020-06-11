#!/bin/bash
#
# 导出数据样例
#

tazk-submit export \
--connect "mongodb://test:test@127.0.0.1:1370/admin" \
--database "test_databse" \
--collection "test_collection" \
--mongo-update-key xxx \
--mongo-update-mode allowInsert \
--mongo-camel-convert true \
--hive-database "default" \
--hive-table "test_table" \
--hive-export-condition "xxx=xxxx" \
--execution-engine "spark" \
--spark-master yarn \
--spark-deploy-mode cluster \
--spark-queue "default" \
--spark-driver-memory "2G" \
--spark-driver-cores 2 \
--spark-executor-memory "2G" \
--spark-num-executors 20 \
--spark-executor-cores 2


