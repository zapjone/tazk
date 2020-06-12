# tazk
mongo和大数据同步工具

# 如何使用

## 导入(import)
```
当增量导入mongo时，可以配置--mongo-import-condition参数，
当以cluster提交到yarn上运行时，必须将参数的json内容进行base64加密，
并通过--mongo-import-condition-encrypt指定。
```
tazk-submit import \
--connect "mongodb://test:test@127.0.0.1:1370/admin" \
--database "test_databse" \
--collection "test_collection" \
--mongo-camel-convert true \
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



## 导出
tazk-submit export \
--connect "mongodb://test:test@127.0.0.1:1370/admin" \
--database "test_databse" \
--collection "test_collection" \
--mongo-update-key xxx \
--mongo-update-mode allowInsert \
--mongo-camel-convert true \
--hive-database "default" \
--hive-table "test_table" \
--hive-export-condition "xxx='xxxx'" \
--execution-engine "spark" \
--spark-master yarn \
--spark-deploy-mode cluster \
--spark-queue "default" \
--spark-driver-memory "2G" \
--spark-driver-cores 2 \
--spark-executor-memory "2G" \
--spark-num-executors 20 \
--spark-executor-cores 2


```
在执行spark引擎导出时，需要在环境配置SPARK_HOME的目录
```