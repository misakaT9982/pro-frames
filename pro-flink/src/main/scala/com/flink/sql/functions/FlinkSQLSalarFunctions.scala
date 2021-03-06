//package com.flink.sql.api
//
//import com.flink.sql.bean.SensorReading
//import com.flink.sql.util.HashCode
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.table.api.Table
//import org.apache.flink.table.api.scala._
//import org.apache.flink.types.Row
//
///**
// * Ciel
// * pro-frames: com.flink.sql.api
// * 2020-11-08 15:48:37
// */
//object FlinkSQLSalarFunctions extends App {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
//    val fileStream = env.readTextFile("D:\\worksapce\\bigdata\\project-flink\\flink-lessons\\src\\main\\resources\\sensor.txt")
//    val dataStream = fileStream.map(data => {
//        val datas = data.split(",")
//        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
//    })
//      .assignTimestampsAndWatermarks(
//          new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
//              override def extractTimestamp(element: SensorReading): Long = {
//                  element.timestamp * 1000L
//              }
//          })
//    // 处理时间最加preceding
//    //val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)
//    // rowtime追加事件事件
//    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
//    val hashCode = new HashCode(1.23)
//
//    // table 调用
//    val resultTable: Table = sensorTable
//      .select('id, 'ts, hashCode('id))
//
//    // SQL
//    tableEnv.createTemporaryView("sensor",sensorTable)
//    tableEnv.registerFunction("hashCode",hashCode)
//    val resultSQLTable = tableEnv.sqlQuery(
//        """
//          |select id, ts, hashCode(id)
//          |from sensor
//          |""".stripMargin)
//
//
//    resultTable.toAppendStream[Row].print("result")
//    resultSQLTable.toAppendStream[Row].print("sql")
//
//    env.execute
//
//}
