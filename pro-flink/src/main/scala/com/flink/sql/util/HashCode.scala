package com.flink.sql.util

import org.apache.flink.table.functions.ScalarFunction


/**
 * Ciel
 * pro-frames: com.flink.sql.util
 * 2020-11-08 15:56:21
 */
class HashCode(factor: Double) extends ScalarFunction {
    def eval(value: String): Int = {
        (value.hashCode * factor).toInt
    }

}
