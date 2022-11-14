package org.example

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

import scala.util.Random

object _15_StreamUnion_Demo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val ds1=env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true){
          Thread.sleep(100)
          ctx.collect(s"other${Random.nextInt(100)}")
        }
      }

      override def cancel(): Unit = {}
    })
    val ds2=env.addSource(new SourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        while (true){
          Thread.sleep(100)
          ctx.collect(Random.nextInt(100))
        }
      }

      override def cancel(): Unit = {}
    })

    //val ds=ds1.map(x=>s"ds1---${x}").union(ds2.map(x=>s"ds2---${x}"))
    //ds.print()

   val ds11=ds1.map(new RichMapFunction[String,String] {
      override def map(value: String): String = {
        val a=getRuntimeContext.getIndexOfThisSubtask
        s"${value}------${a}"
      }
    }).disableChaining()
env.getConfig.setAutoWatermarkInterval(0)
    ds11.map(_.length).map(_.toString).disableChaining().print()

    ds11.map(_.length).map(_.toString).startNewChain().print()



    env.execute()

  }
}
