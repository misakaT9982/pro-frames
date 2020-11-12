package com.flink.sql.api

import com.flink.sql.bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Ciel
 * pro-frames: com.flink.sql.api
 * 2020-11-08 12:06:43
 */
object FlinkSQLTimeWindows {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val fileStream: DataStream[String] = env.readTextFile("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
        val datas: Array[String] = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })
      .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(10)) {
              override def extractTimestamp(element: SensorReading): Long = {
                  element.timestamp * 1000L
              }
          }
      )
    // 处理时间直接proctime
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)

    // Group Windows操作
    val resultGroupTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('dt, 'tw)
      .select('id, 'id.count, 'tw.end)

    // Over Windows操作
    val overResultWindow: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperatrue.avg over 'ow)

    // SQL实现Group By
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSQLTable: Table = tableEnv.sqlQuery(
        """
          |select id, count(id), hop_end(ts, interval '4' second, interval '10' second)
          |from sensor
          |group by id, hop(ts, interval '4' second, interval '10' second)
          |""".stripMargin)

    // Over Window
    val overSQLTable: Table = tableEnv.sqlQuery(
        """
          | select id, count(id) over w, avg(temperature) over w
          | from sensor
          | window w as (
          |     partition by id
          |     order by ts
          |     rows between w preceding and current row
          | )
          |""".stripMargin)


    //resultTable.toRetractStream[Row].print("agg")
    //overResultTable.toAppendStream[Row].print("over")
    resultSQLTable.toAppendStream[Row].print("aggSQL")
    overSQLTable.toAppendStream[Row].print("overSQL")

    //sensorTable.printSchema()
    env.execute
}
