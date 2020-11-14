package com.flink.sql.functions

import com.flink.sql.bean.SensorReading
import com.flink.sql.util.TableFunctionStr
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
 * 2020-11-14 16:28:04
 */
object FlinkSQLTableFunction {
    def main(args: Array[String]): Unit = {
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
        // 将流转换成表，直接定义时间字段。rowtime追加事件事件
        val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

        //创建一个UDF对象
        val split = new TableFunctionStr("_")
        //Table API调用
        val resultTable = sensorTable
          .joinLateral(split('id) as ('word, 'length)) // as 拆分后的列别名
          // .leftOuterJoinLateral()
          .select('id, 'ts, 'word, 'length)
        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable)
        tableEnv.registerFunction("split", split)

        val resultSQLTable = tableEnv.sqlQuery(
            """
              |select id, ts, word, length
              |from sensor, lateral table (split(id)) as split_id(word, length)
              |""".stripMargin)


        resultTable.toAppendStream[Row].print("result")
        resultSQLTable.toAppendStream[Row].print("SQL")
        env.execute()
    }

}
