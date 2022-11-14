package org.example

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import java.lang
import java.text.SimpleDateFormat
import java.util.Date

object _20_Window_api_demo {
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
      .aggregate(new AggregateFunction[EventLog,Long,Long] {
        override def createAccumulator(): Long ={
          println("---------createAccumulator---")
          0L
        }

        override def add(value: EventLog, accumulator: Long): Long = {
          println(s"----------add ${accumulator}+1----------")
          accumulator+1
        }

        override def getResult(accumulator: Long): Long = {
          println(s"-----getResult ${accumulator}--------")
          accumulator
        }

        override def merge(a: Long, b: Long): Long = {
          println(s"--------merge-----${a}+${b}------------")
          a+b
        }
      })
      .print()



//
//     source .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .process(new ProcessWindowFunction[EventLog,String,String,TimeWindow]{
//        override def process(key: String, context: Context, elements: Iterable[EventLog], out: Collector[String]): Unit = {
//          out.collect(s"没有延迟${elements.size}")
//        }
//      })
//      .print()
//
//    val tag=new OutputTag[EventLog]("tag")
//
//    val ds1=source.window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .allowedLateness(Time.seconds(3))
//      .sideOutputLateData(tag)
//      .process(new ProcessWindowFunction[EventLog,String,String,TimeWindow]{
//        override def process(key: String, context: Context, elements: Iterable[EventLog], out: Collector[String]): Unit = {
//          out.collect(s"有延迟${elements.size}")
//        }
//      })
//    ds1.getSideOutput(tag).print()
//      ds1
//      .print()


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

