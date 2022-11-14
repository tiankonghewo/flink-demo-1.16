package org.example

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._


object _23_Event_api_demo {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  case class EventLog(ts:Long,t:String,mytype:String="a")
  class MyMap extends MapFunction[String,EventLog]{
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    override def map(value: String): EventLog = {
      val ts=sdf.parse(value).getTime
      //println(s"------mymap----------${Thread.currentThread().getId}")
      EventLog(ts,value)
    }
  }
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
    ).map{
      x=>
        val y=x
        (x.mytype,x.t)
    }
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new ProcessWindowFunction[(String,String),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[String]): Unit = {
          elements.foreach(x=>out.collect(x._2))
        }
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forMonotonousTimestamps[String].withTimestampAssigner(
          new SerializableTimestampAssigner[String] {
            override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
              println(s"------------${element}---${sdf.format(new Date(recordTimestamp))}---------------")
              recordTimestamp
            }
          }
        )
      ).process(new ProcessFunction[String,String]{
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        out.collect(value)
      }
    })

      .print()


    env.execute()
  }
}

