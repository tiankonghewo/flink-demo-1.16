package org.example
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.util.function.SerializableSupplier

import scala.util.Random

object _14_StreamConnect_Demo {

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

    val ds=ds1.connect(ds2)

    ds.map(new CoMapFunction[String,Int,String] {
      override def map1(value: String): String = {
        s"ds1--${value}"
      }

      override def map2(value: Int): String = {
        s"ds2--${value}"
      }
    }).print()


    env.execute()

  }
}
