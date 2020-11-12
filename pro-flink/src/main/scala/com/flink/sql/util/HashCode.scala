package com.flink.sql.util

import org.apache.flink.table.functions.ScalarFunction

/**
 * Ciel
 * pro-frames: com.flink.sql.util
 * 2020-11-08 15:56:21
 */
class HashCode(factor: Int) extends ScalarFunction {
    def elv(s: String):Int = {
        s.hashCode * factor
    }

}
