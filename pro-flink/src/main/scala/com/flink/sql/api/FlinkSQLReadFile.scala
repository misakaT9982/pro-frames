package com.flink.sql.api

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
 * Ciel
 * pro-frames: com.flink.sql.api
 * 2020-11-07 18:15:34
 */
object FlinkSQLReadFile extends App {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取文件系统
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val filePath = "D:\\worksapce\\frame\\pro-frames\\pro-flink\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv)
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable") // 注册结构表
    val inputTable = tableEnv.from("inputTable")
    // 读取kafka
    tableEnv.connect(
        new Kafka()
          .version("0.11")
          .topic("sensor")
          .property("zookeeper.connect", "hadoop102:2181")
          .property("bootstrap.service", "hadoop102:9092")
    )
      .withFormat(new Csv)
      .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("tempareture", DataTypes.DOUBLE())
      )
      .createTemporaryTable("KafkaInputTable")
    // 表的查询和转换
    val sensorTable = tableEnv.from("inputTable")
    // 简单查询
    val resultKafkaTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")
    //tableEnv.sqlQuery("select id, temperature from ")
    // 聚合查询
    val aggResultTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)
    val aggSQLResultTable = tableEnv.sqlQuery("select id, count(id) count_id from inputTable group by id")
    //resultKafkaTable.toAppendStream[(String, Double)].print("res ->")
    aggResultTable.toRetractStream[(String, Long)].print("agg -> ")
    //aggSQLResultTable.toRetractStream[(String,Double)]

    val resultTable: DataStream[(String, Long, Double)] = inputTable.toAppendStream[(String, Long, Double)]
    //resultTable.print()
    env.execute("sql api file job")
}
