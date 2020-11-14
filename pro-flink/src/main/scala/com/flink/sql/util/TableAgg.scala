package com.flink.sql.util


import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
 * Ciel
 * pro-frames: com.flink.sql.util
 * 2020-11-14 17:06:46
 */
class TableAgg extends TableAggregateFunction[(Double, Int), TableAcc] {
    override def createAccumulator(): TableAcc = new TableAcc()

    // 每来一个数据
    def accumulate(acc: TableAcc, temp: Double): Unit = {
        // 将当前温度值与状态中的最高温度和第二温度比较，如果大于就替换
        if (temp > acc.hightStemp) {
            acc.secondsHightStemp = acc.hightStemp
            acc.hightStemp = temp
        } else if (temp > acc.secondsHightStemp) {
            acc.secondsHightStemp = temp
        }
    }

    // 输出数据的方法，写入结果表
    def emitValue(acc: TableAcc, out: Collector[(Double, Int)]): Unit = {
        out.collect((acc.hightStemp, 1))
        out.collect((acc.secondsHightStemp, 2))
    }
}
