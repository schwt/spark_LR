#!/bin/sh
#!encoding:utf-8
# . ~/.profile

SPARK_HOME=""
SPARK_BIN="${SPARK_HOME}/bin"

HADOOP_HOME=""
HADOOP_BIN="${HADOOP_HOME}/bin"


HDFS_input="hdfs://train_set/day=2017-09-29,\
hdfs://dir_input/day=2017-10-16"

HDFS_output="hdfs://dir_output/model"


hadoop fs -rm -r $HDFS_output

dim=20
epoche=3
scan_time=5
lambda=0.005
eta0=0.1
parts=1000

# ${SPARK_BIN}/spark-submit                   \
spark-submit                                \
    --name "wyb_spark_test"                 \
    --master yarn                           \
    --deploy-mode cluster                   \
    --num-executors 80                     \
    --driver-memory 5g                      \
    --executor-memory 10g                   \
    --conf spark.default.parallelism=500    \
    --conf spark.yarn.maxAppAttempts=1      \
    --conf spark.shuffle.memoryFraction=0.5 \
    --class sgd4spark.work.manage           \
    --jars fastutil-7.2.1.jar               \
    ./sgd_test.jar ${HDFS_input} ${HDFS_output} $dim $epoche $scan_time $lambda $eta0 $parts
    # --conf spark.yarn.executor.memoryOverhead=7000      \
    

