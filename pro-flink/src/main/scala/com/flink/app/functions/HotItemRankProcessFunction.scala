package com.flink.app.functions

import com.flink.app.bean.ItemClickCount
import java.sql.Timestamp
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * mito
 * pro-frames: com.flink.app.functions
 * 2020-11-28 19:09:27
 */
class HotItemRankProcessFunction(num: Int) extends ProcessFunction[ItemClickCount, String] {
    private var itemCountList: ListState[ItemClickCount] = _
    private var calcTimer: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        itemCountList = getRuntimeContext
          .getListState[ItemClickCount](
              new ListStateDescriptor[ItemClickCount]("itemListCount", classOf[ItemClickCount])
          )
        calcTimer = getRuntimeContext
          .getState[Long](
              new ValueStateDescriptor[Long]("calcTimer", classOf[Long])
          )
    }

    // 将每一次进入分区的数据进行状态的保存
    override def processElement(value: ItemClickCount, ctx: ProcessFunction[ItemClickCount, String]#Context, out: Collector[String]): Unit = {
        itemCountList.add(value)
        if (calcTimer.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEndTime)
            calcTimer.update(value.windowEndTime)
        }
    }

    // 定时器到达时出发窗口计算
    override def onTimer(timestamp: Long, ctx: ProcessFunction[ItemClickCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        val buffer = new ListBuffer[ItemClickCount]
        // 从对象中获取数据
        val counts = itemCountList.get()
        import scala.collection.JavaConversions._
        counts.iterator().foreach(item =>{
            buffer.append(item)
        })
        // 清楚状态
        itemCountList.clear()
        // 对数据降序操作取出前N
        val iccList = buffer.sortBy(_.count)(Ordering.Long.reverse).take(num)
        val builder = new StringBuilder
        builder.append("时间范围" + new Timestamp(timestamp) + "\n")
        iccList.foreach(item =>{
            builder.append("商品ID：" + item.itemId + ", 点击次数：" + item.count + "\n")
        })
        builder.append("======================")
        Thread.sleep(1000)
        out.collect(builder.toString())
    }
}
