package org.example

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object _16_StreamJoin_Demo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val ds1=env.addSource(new SourceFunction[(String,String)] {
      override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
        while (true){
          Thread.sleep(1000)
          val x=Random.nextInt(100)
          ctx.collect((x%3).toString,s"ds1--${x}")
        }
      }

      override def cancel(): Unit = {}
    })
    val ds2=env.addSource(new SourceFunction[(String,String)] {
      override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
        while (true){
          Thread.sleep(1000)
          val x=Random.nextInt(100)
          ctx.collect((4).toString,s"ds2--${x}")
        }
      }

      override def cancel(): Unit = {}
    })

    ds1.join(ds2).where(_._1).equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .apply(new JoinFunction[(String,String),(String,String),String] {
        override def join(first: (String, String), second: (String, String)): String = {
          s"${first}----${second}"
        }
      })
      .print()



    env.execute()

  }
}
