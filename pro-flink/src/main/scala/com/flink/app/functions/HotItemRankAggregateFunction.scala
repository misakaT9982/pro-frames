package com.flink.app.functions

import com.flink.app.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * mito
 * pro-frames: com.flink.app.functions
 * 2020-11-28 19:03:03
 */
class HotItemRankAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = {
        accumulator + 1L
    }

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}
