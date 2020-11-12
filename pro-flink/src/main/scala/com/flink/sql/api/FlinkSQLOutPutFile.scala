package com.flink.sql.api

import com.flink.sql.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * Ciel
 * pro-frames: com.flink.sql.api
 * 2020-11-08 11:15:49
 */
object FlinkSQLOutPutFile extends App {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    val inputDStream = env.readTextFile("D:\\worksapce\\frame\\pro-frames\\pro-flink\\src\\main\\resources\\sensor.txt")
    val dataDStream = inputDStream.map(data => {
        val datas = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })
    val sensorTable: Table = tableEnv.fromDataStream(dataDStream, 'id, 'timestamp as 'ts, 'temperature as 'temp)
    val resultTale = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    //    val aggResultTable = sensorTable
    //      .groupBy('id)
    //      .select('id, 'id.count as 'cnt)
    tableEnv.connect(new FileSystem().path("D:\\worksapce\\frame\\pro-frames\\pro-flink\\src\\main\\resources\\output.txt"))
      .withFormat(new Csv())
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")
    resultTale.insertInto("outputTable")
    env.execute("output file")
}
