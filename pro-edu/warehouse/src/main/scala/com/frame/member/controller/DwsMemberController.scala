package com.frame.member.controller

import com.frame.member.service.DwsMemberService
import com.frame.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * mito
 * pro-frames: com.frame.member.controller
 * 2020-12-08 21:38:58
 */
object DwsMemberController extends App {
    val sparkConf = new SparkConf().setAppName("DwsMemberController")
    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val ssc  =sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompress(sparkSession)

    DwsMemberService.importMemberUseSQL(sparkSession,"20190722")
    DwsMemberService.importMemberUseApi(sparkSession,"20190722")


}
