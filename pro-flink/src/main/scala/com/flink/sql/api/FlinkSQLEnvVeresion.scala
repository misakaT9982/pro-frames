package com.flink.sql.api

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

/**
 * Ciel
 * pro-frames: com.flink.sql.api
 * 2020-11-07 18:00:42
 */
object FlinkSQLEnvVeresion extends App {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() // 版本选择
      .useOldPlanner() // 老版本
      .inStreamingMode() // 流处理模式
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env,settings)
    // 老版本的批处理环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchEnv)
    // blink版本的流式处理
    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkTableEnv = StreamTableEnvironment.create(env,blinkSettings)
    // blink版本的批处理环境
    val blinkBatchEnv = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchEnv)


}
