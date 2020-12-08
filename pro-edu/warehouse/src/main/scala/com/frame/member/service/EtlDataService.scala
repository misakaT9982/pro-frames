package com.frame.member.service

import com.alibaba.fastjson.JSONObject
import com.frame.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * mito
 * pro-frames: com.frame.member.service
 * 2020-12-08 21:10:04
 */
object EtlDataService {

    /**
     * 导入广告表基础数据
     *
     * @param ssc
     * @param sparkSession
     */
    def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
        import sparkSession.implicits._
        val result = ssc.textFile("/user/atguigu/ods/baseadlog.log").filter(item => {
            val obj = ParseJsonData.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partition => {
            partition.map(item => {
                val jsonObject = ParseJsonData.getJsonObject(item)
                val adid: Int = jsonObject.getIntValue("adid")
                val adname: String = jsonObject.getString("adname")
                val dn: String = jsonObject.getString("dn")
                (adid, adname, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
    }

    /**
     * 导入网站表基础数据
     *
     * @param ssc
     * @param sparkSession
     */
    def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        val result = ssc.textFile("/user/atguigu/ods/baswewebsite.log").filter(item => {
            val obj = ParseJsonData.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partition => {
            partition.map(item => {
                val jsonObject = ParseJsonData.getJsonObject(item)
                val siteid = jsonObject.getIntValue("siteid")
                val sitename = jsonObject.getString("sitename")
                val siteurl = jsonObject.getString("siteurl")
                val delete = jsonObject.getIntValue("delete")
                val createtime = jsonObject.getString("createtime")
                val creator = jsonObject.getString("creator")
                val dn = jsonObject.getString("dn")
                (siteid, sitename, siteurl, delete, createtime, creator, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
    }

    /**
     * 导入用户数据
     *
     * @param ssc
     * @param sparkSession
     */
    def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
        import sparkSession.implicits._
        val result = ssc.textFile("").filter(item => {
            val obj = ParseJsonData.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partition => {
            partition.map(item => {
                val jsonObject = ParseJsonData.getJsonObject(item)
                val ad_id = jsonObject.getIntValue("ad_id")
                val birthday = jsonObject.getString("birthday")
                val email = jsonObject.getString("email")
                val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
                val iconurl = jsonObject.getString("iconurl")
                val lastlogin = jsonObject.getString("lastlogin")
                val mailaddr = jsonObject.getString("mailaddr")
                val memberlevel = jsonObject.getString("memberlevel")
                val password = "******"
                val paymoney = jsonObject.getString("paymoney")
                val phone = jsonObject.getString("phone")
                val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
                val qq = jsonObject.getString("qq")
                val register = jsonObject.getString("register")
                val regupdatetime = jsonObject.getString("regupdatetime")
                val uid = jsonObject.getIntValue("uid")
                val unitname = jsonObject.getString("unitname")
                val userip = jsonObject.getString("userip")
                val zipcode = jsonObject.getString("zipcode")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
                  register, regupdatetime, unitname, userip, zipcode, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_member")
    }

    /**
     * 导入用户注册信息
     *
     * @param ssc
     * @param sparkSession
     */
    def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
        import sparkSession.implicits._
        val result = ssc.textFile("").filter(item => {
            val obj = ParseJsonData.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partition => {
            partition.map(item => {
                val jsonObject = ParseJsonData.getJsonObject(item)
                val appkey = jsonObject.getString("appkey")
                val appregurl = jsonObject.getString("appregurl")
                val bdp_uuid = jsonObject.getString("bdp_uuid")
                val createtime = jsonObject.getString("createtime")
                val domain = jsonObject.getString("webA")
                val isranreg = jsonObject.getString("isranreg")
                val regsource = jsonObject.getString("regsource")
                val regsourceName = regsource match {
                    case "1" => "PC"
                    case "2" => "Mobile"
                    case "3" => "App"
                    case "4" => "WeChat"
                    case _ => "other"
                }
                val uid = jsonObject.getIntValue("uid")
                val websiteid = jsonObject.getIntValue("websiteid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_member_regtype")
    }

    /**
     * 导入用户付款信息
     *
     * @param ssc
     * @param sparkSession
     */
    def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
        import sparkSession.implicits._
        val result = ssc.textFile("").filter(item => {
            val obj = ParseJsonData.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partition => {
            partition.map(item => {
                val jsonObject = ParseJsonData.getJsonObject(item)
                val paymoney = jsonObject.getString("paymoney")
                val uid = jsonObject.getIntValue("uid")
                val vip_id = jsonObject.getIntValue("vip_id")
                val site_id = jsonObject.getIntValue("siteid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (uid, paymoney, site_id, vip_id, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_pcentermempaymoney")
    }

    def etlMemVipLevelLog(ssc:SparkContext,sparkSession: SparkSession): Unit ={
        import sparkSession.implicits._
        val result = ssc.textFile("").filter(item =>{
            val obj = ParseJsonData.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partition =>{
            partition.map(item =>{
                val jsonObject = ParseJsonData.getJsonObject(item)
                val discountval = jsonObject.getString("discountval")
                val end_time = jsonObject.getString("end_time")
                val last_modify_time = jsonObject.getString("last_modify_time")
                val max_free = jsonObject.getString("max_free")
                val min_free = jsonObject.getString("min_free")
                val next_level = jsonObject.getString("next_level")
                val operator = jsonObject.getString("operator")
                val start_time = jsonObject.getString("start_time")
                val vip_id = jsonObject.getIntValue("vip_id")
                val vip_level = jsonObject.getString("vip_level")
                val dn = jsonObject.getString("dn")
                (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
    }
}
