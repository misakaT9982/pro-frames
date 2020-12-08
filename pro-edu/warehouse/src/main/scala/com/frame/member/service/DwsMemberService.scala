package com.frame.member.service

import com.frame.member.bean.{DwsMember, DwsMember_Result, MemberZipper, MemberZipperResult}
import com.frame.member.dao.DwdMemberDao
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * mito
 * pro-frames: com.frame.member.service
 * 2020-12-08 21:41:51
 */
object DwsMemberService {
    def importMemberUseSQL(sparkSession: SparkSession, time: String) = {
        import sparkSession.implicits._
        sparkSession.sql(
            s"""
               |select
               |    uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin),
               |    first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq),
               |    first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode),
               |    first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain),
               |    first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename),
               |    first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level),
               |    min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level),
               |    first(vip_operator),dt,dn
               |from
               |       (
               |            select
               |                a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel,
               |                a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip,
               |                a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource,
               |                b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime,
               |                d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time,
               |                f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free,
               |                f.next_level as vip_next_level,f.operator as vip_operator,a.dn
               |            from
               |                dwd.dwd_member a
               |                left join dwd.dwd_member_regtype b on a.uid=b.uid  and a.dn=b.dn
               |                left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
               |                left join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
               |                left join dwd.dwd_pcentermempaymoney e on a.uid=e.uid and a.dn=e.dn
               |                left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn
               |            where a.dt='${time}'
               |        )
               |group by uid,dn,dt""".stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")
        // 查询当天增量数据
        val dayReulst = sparkSession.sql(
            s"""
               |select
               |    a.uid,
               |    sum(cast(a.paymoney as decimal(10,4))) as paymoney,
               |    max(b.vip_level) as vip_level,
               |    from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
               |    '9999-12-31' as end_time,
               |    first(a.dn) as dn
               |from
               |    dwd.dwd_pcentermempaymoney a
               |left join b on dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
               |where dt = '$time'
               |group by uid""".stripMargin).as[MemberZipper]
        // 查询历史拉链表数据
        val historyResult = sparkSession.sql("select * from dws.dws_member_zipper").as[MemberZipper]
        //两份数据根据用户id进行聚合 对end_time进行重新修改
        val result = dayReulst.union(historyResult)
          .groupByKey(item => item.uid + "_" + item.dn)
          .mapGroups {
              case (key, iter) =>
                  val list = iter.toList.sortBy(item => item.start_time)
                  if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
                      val oldLastModel = list(list.size - 2)

                      val lastModel = list(list.size - 1)
                      oldLastModel.end_time = lastModel.start_time
                      lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal(oldLastModel.paymoney)).toString()
                  }
                  MemberZipperResult(list)
          }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")
    }

    /**
     * 使用API分析
     *
     * @param sparkSession
     * @param time
     */
    def importMemberUseApi(sparkSession: SparkSession, time: String): Unit = {
        import sparkSession.implicits._
        val dwdMember = DwdMemberDao.getDwdMember(sparkSession).where(s"dt = '${time}'")
        val dwdMemberRegtype = DwdMemberDao.getDwdMemberRegType(sparkSession)
        val dwdBaseAd: DataFrame = DwdMemberDao.getDwdBaseAd(sparkSession)
        val dwdBaseWebsite: DataFrame = DwdMemberDao.getDwdBaseWebSite(sparkSession)
        val dwdPcentermemPaymoney: DataFrame = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
        val dwdVipLevel: DataFrame = DwdMemberDao.getDwdVipLevel(sparkSession)
        val result = dwdMember.join(dwdMemberRegtype, Seq("uid", "dn"), "left")
          .join(dwdBaseAd, Seq("ad_id", "dn"), "left_outer")
          .join(dwdBaseWebsite, Seq("siteid", "dn"), "left_outer")
          .join(dwdPcentermemPaymoney, Seq("uid", "dn"), "left_outer")
          .join(dwdVipLevel, Seq("vip_id", "dn"), "left_outer")
          .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
              , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
              , "appregurl", "bdp_uuid", "reg_createtime", "domain", "isranreg", "regsource", "regsourcename", "adname"
              , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
              "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
              , "vip_operator", "dt", "dn").as[DwsMember]
        val resultDataSet = result.groupByKey(item => item.uid + "_" + item.dn)
          .mapGroups { case (key, iters) =>
              val keys = key.split("_")
              val uid = Integer.parseInt(keys(0))
              val dn = keys(1)
              val dwsMembers = iters.toList
              val paymoney = dwsMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString
              val ad_id = dwsMembers.map(_.ad_id).head
              val fullname = dwsMembers.map(_.fullname).head
              val icounurl = dwsMembers.map(_.iconurl).head
              val lastlogin = dwsMembers.map(_.lastlogin).head
              val mailaddr = dwsMembers.map(_.mailaddr).head
              val memberlevel = dwsMembers.map(_.memberlevel).head
              val password = dwsMembers.map(_.password).head
              val phone = dwsMembers.map(_.phone).head
              val qq = dwsMembers.map(_.qq).head
              val register = dwsMembers.map(_.register).head
              val regupdatetime = dwsMembers.map(_.regupdatetime).head
              val unitname = dwsMembers.map(_.unitname).head
              val userip = dwsMembers.map(_.userip).head
              val zipcode = dwsMembers.map(_.zipcode).head
              val appkey = dwsMembers.map(_.appkey).head
              val appregurl = dwsMembers.map(_.appregurl).head
              val bdp_uuid = dwsMembers.map(_.bdp_uuid).head
              val reg_createtime = dwsMembers.map(_.reg_createtime).head
              val domain = dwsMembers.map(_.domain).head
              val isranreg = dwsMembers.map(_.isranreg).head
              val regsource = dwsMembers.map(_.regsource).head
              val regsourcename = dwsMembers.map(_.regsourcename).head
              val adname = dwsMembers.map(_.adname).head
              val siteid = dwsMembers.map(_.siteid).head
              val sitename = dwsMembers.map(_.sitename).head
              val siteurl = dwsMembers.map(_.siteurl).head
              val site_delete = dwsMembers.map(_.site_delete).head
              val site_createtime = dwsMembers.map(_.site_createtime).head
              val site_creator = dwsMembers.map(_.site_creator).head
              val vip_id = dwsMembers.map(_.vip_id).head
              val vip_level = dwsMembers.map(_.vip_level).max
              val vip_start_time = dwsMembers.map(_.vip_start_time).min
              val vip_end_time = dwsMembers.map(_.vip_end_time).max
              val vip_last_modify_time = dwsMembers.map(_.vip_last_modify_time).max
              val vip_max_free = dwsMembers.map(_.vip_max_free).head
              val vip_min_free = dwsMembers.map(_.vip_min_free).head
              val vip_next_level = dwsMembers.map(_.vip_next_level).head
              val vip_operator = dwsMembers.map(_.vip_operator).head
              DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
                  phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
                  bdp_uuid, reg_createtime, domain, isranreg, regsource, regsourcename, adname, siteid,
                  sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
                  vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
                  vip_next_level, vip_operator, time, dn)
          }
        resultDataSet.show()
    }

}