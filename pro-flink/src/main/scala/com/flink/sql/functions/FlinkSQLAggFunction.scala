package com.flink.sql.functions

import com.flink.sql.bean.SensorReading
import com.flink.sql.util.AggregateFunc
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Ciel
 * pro-frames: com.flink.sql.functions
 * 2020-11-14 16:46:11
 */
object FlinkSQLAggFunction extends App {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val fileStream: DataStream[String] = env.readTextFile("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
        val datas: Array[String] = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })
      .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
              override def extractTimestamp(element: SensorReading): Long = {
                  element.timestamp * 1000L
              }
          })
    // 处理时间最加preceding
    //val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)
    // rowtime追加事件事件
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
    val avgTmp = new AggregateFunc()

    // Table API 调用
    val resultTable = sensorTable
      //.feiy ==
      .groupBy('id)
      .aggregate(avgTmp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)

    // SQL
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTmp)

    resultTable.toRetractStream[Row].print("result")

    env.execute()

}
