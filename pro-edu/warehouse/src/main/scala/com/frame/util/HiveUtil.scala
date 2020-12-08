package com.frame.util

import org.apache.spark.sql.SparkSession

/**
 * mito
 * pro-frames: com.frame.util
 * 2020-12-08 21:07:10
 */
object HiveUtil {


    /**
     * 开启动态分区并开启非严格模式
     * @param spark
     */
    def openDynamicPartition(spark:SparkSession): Unit ={
        spark.sql("set hive.exec.dynamic.partition = true")
        spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    }

    def openCompress(spark:SparkSession): Unit ={
        spark.sql("set mapred.output.compress = true")
        spark.sql("set hive.exec.output.compress = true")
    }
}
