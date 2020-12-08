package com.flink.app.functions

import java.lang

import com.flink.app.bean.ItemClickCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * mito
 * pro-frames: com.flink.app.functions
 * 2020-11-28 19:03:10
 */
class HotItemWindowFunction extends WindowFunction[Long, ItemClickCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemClickCount]): Unit = {
        out.collect(ItemClickCount(key, input.iterator.next(), window.getEnd))
    }
}
