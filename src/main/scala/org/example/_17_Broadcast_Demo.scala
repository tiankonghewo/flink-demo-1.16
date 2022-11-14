package org.example

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.util.Random

object _17_Broadcast_Demo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val ds1=env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while(true){
          Thread.sleep(1000)
          ctx.collect(s"${Random.nextInt(10)}")
        }
      }

      override def cancel(): Unit ={}
    })
    val ds2=env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while(true){
          Thread.sleep(1000)
          ctx.collect(s"${Random.nextInt(10)}")
        }
      }

      override def cancel(): Unit ={}
    })


    val desc=new MapStateDescriptor[String, String]("order_conf_broadcast", createTypeInformation[String],
      createTypeInformation[String])
      val ds22= ds2.broadcast(desc)


    ds1.connect(ds22).process(
      new BroadcastProcessFunction[String,String,String]{
        override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
          val b=ctx.getBroadcastState(desc)
          if(b!=null){
            val c=b.get(value)
            if(c!=null){
              out.collect(value+c)
            }

          }

        }

        override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
          ctx.getBroadcastState(desc).put(value,s"广播流${value}")
        }
      }
    ).print()
    env.execute()

  }
}
