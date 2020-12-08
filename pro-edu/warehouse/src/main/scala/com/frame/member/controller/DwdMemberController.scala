package com.frame.member.controller

import com.frame.member.service.EtlDataService
import com.frame.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * mito
 * pro-frames: com.frame.member.controller
 * 2020-12-08 21:05:17
 */
object DwdMemberController extends App {
    val sparkConf: SparkConf = new SparkConf().setAppName("DwdMemberController")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompress(sparkSession)

    EtlDataService.etlBaseAdLog(ssc,sparkSession)
    EtlDataService.etlBaseWebSiteLog(ssc, sparkSession) //导入基础网站表数据
    EtlDataService.etlMemberLog(ssc, sparkSession) //清洗用户数据
    EtlDataService.etlMemberRegtypeLog(ssc, sparkSession) //清洗用户注册数据
    EtlDataService.etlMemPayMoneyLog(ssc, sparkSession) //导入用户支付情况记录
    EtlDataService.etlMemVipLevelLog(ssc, sparkSession) //导入vip基础数据

}
