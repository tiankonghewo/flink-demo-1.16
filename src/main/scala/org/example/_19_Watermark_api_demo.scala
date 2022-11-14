package org.example
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date

object _19_Watermark_api_demo {
  val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  case class EventLog(ts:Long,t:String)
  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    env.setParallelism(1)
    val source=env.socketTextStream("localhost",9999)
    source.filter(_.nonEmpty).map(new MyMap).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[EventLog].withTimestampAssigner(
      new SerializableTimestampAssigner[EventLog] {
        override def extractTimestamp(element: EventLog, recordTimestamp: Long): Long = element.ts
      }
    )
    )
      .process(new ProcessFunction[EventLog,EventLog] {
      override def processElement(value: EventLog, ctx: ProcessFunction[EventLog, EventLog]#Context, out: Collector[EventLog]): Unit = {
        out.collect(value)
      }
    })    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
      .process(new ProcessAllWindowFunction[EventLog,EventLog,TimeWindow] {
        var i=0
        override def process(context: Context, elements: Iterable[EventLog], out: Collector[EventLog]): Unit = {
          i+=1
          println(s"--------第一个窗口第${i}次触发----[${sdf.format(new Date(context.window.getStart))},${sdf.format(new Date(context.window.getEnd))})----")
          elements.foreach(out.collect)
        }
      })
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new ProcessAllWindowFunction[EventLog,Int,TimeWindow] {
        var i=0
        override def process(context: Context, elements: Iterable[EventLog], out: Collector[Int]): Unit = {
          i+=1
          println(s"--------第二个窗口第${i}次触发---${elements.toList}--")
          out.collect(elements.size)
        }
      })
      .print()

    env.execute()
  }

  class MyMap extends MapFunction[String,EventLog]{
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    override def map(value: String): EventLog = {
      val ts=sdf.parse(value).getTime
      println(s"------mymap----------${Thread.currentThread().getId}")
      EventLog(ts,value)
    }
  }
}

