package com.flink.sql.api

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * Ciel
 * pro-frames: com.flink.sql.api
 * 2020-11-08 11:51:17
 */
object FlinkSQLKafkaPipe extends App {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    // 输入表
    tableEnv.connect(
        new Kafka()
          .version("0.11")
          .topic("sensor")
          .property("zookeeper.connect", "hadoop102:2181")
          .property("bootstrap.service", "hadoop102:9092")
    )
      .withFormat(new Csv())
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    val aggResultTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 输出表
    tableEnv.connect(
        new Kafka()
          .version("0.11")
          .topic("sensor")
          .property("zookeeper.connect", "hadoop102:2181")
          .property("bootstrap.service", "hadoop102:9092")
    )
      .withFormat(new Csv())
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutPutTable")

    resultTable.insertInto("kafkaOutPutTable")
    env.execute()
}
