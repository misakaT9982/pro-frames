package com.flink.sql.util

import org.apache.flink.table.functions.AggregateFunction

/**
 * Misaka
 * pro-frames: com.flink.sql.util
 * 2020-11-14 16:47:48
 */
class AggregateFunc extends AggregateFunction[Double, AccumulateFuns] {

    override def getValue(accumulator: AccumulateFuns): Double = accumulator.sum / accumulator.count

    override def createAccumulator(): AccumulateFuns = new AccumulateFuns

    def accumulate(acc: AccumulateFuns, temp: Double): Unit = {
        acc.sum += temp
        acc.count += 1

    }
}

