package com.flink.sql.util

import org.apache.flink.table.functions.TableFunction

/**
 * 自定义TableFunction，实现分割字符串并统计长度(word, length)
 * Ciel
 * pro-frames: com.flink.sql.util
 * 2020-11-14 16:31:32
 */
class TableFunctionStr(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
        str.split(separator).foreach(
            word => collect((word, word.length))
        )
    }

}
