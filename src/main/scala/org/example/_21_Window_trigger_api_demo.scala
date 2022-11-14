package org.example

import com.flink.demo.MyTrigger
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat


object _21_Window_trigger_api_demo {
  val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  case class EventLog(ts:Long,t:String,mytype:String="a")
  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    env.setParallelism(1)
    val source=env.socketTextStream("localhost",9999)
    .filter(_.nonEmpty).map(new MyMap).assignTimestampsAndWatermarks(
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
    }) .keyBy(_.mytype)




     source .window(TumblingEventTimeWindows.of(Time.seconds(10)))
       .trigger(new MyTrigger)
      .process(new ProcessWindowFunction[EventLog,String,String,TimeWindow]{
        override def process(key: String, context: Context, elements: Iterable[EventLog], out: Collector[String]): Unit = {
          out.collect(s"没有延迟${elements.size}")
        }
      })
      .print()



    env.execute()
  }

  class MyMap extends MapFunction[String,EventLog]{
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    override def map(value: String): EventLog = {
      val ts=sdf.parse(value).getTime
      //println(s"------mymap----------${Thread.currentThread().getId}")
      EventLog(ts,value)
    }
  }

}

