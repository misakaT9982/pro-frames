package com.flink.sql.api

import com.flink.sql.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * Ciel
 * pro-frames: com.flink.sql
 * 2020-11-07 17:20:47
 */
object FlinkSQLPractice {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val fileData: DataStream[String] = env.readTextFile("D:\\worksapce\\frame\\pro-frames\\pro-flink\\src\\main\\resources\\sensor.txt")
        val dataStream = fileData.map(data => {
            val datas = data.split(",")
            SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
        })
        // 1. 基于table创建环境
        val tableEnv = StreamTableEnvironment.create(env)
        // 2. 基于tableEnv将流转换成表，注册表
        val dataTable: Table = tableEnv.fromDataStream(dataStream)
        val resultTable: Table = dataTable
          .select("id,temperature")
          .filter("id == 'sensor_1'")
        // SQL实现
        tableEnv.registerTable("dataTable", dataTable)
        val resultSQLTable = tableEnv.sqlQuery(
            """
              |select id, temperature
              |from dataTable
              |where id = 'sensor_1'
              |""".stripMargin)
        // 拼接字符串会产生序列化，底层会默认构建一个view
        // tableEnv.sqlQuery("select id, temperature from " + dataTable + "where id = 'sensor_1")

        val resultDStream = resultTable.toAppendStream[(String, Double)]
         resultDStream.print()
        val resultSql = resultSQLTable.toAppendStream[(String, Double)]
        //resultSql.print()
        env.execute("sql api job")
    }
}
