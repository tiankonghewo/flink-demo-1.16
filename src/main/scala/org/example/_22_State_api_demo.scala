package org.example

import com.flink.demo.MyTrigger
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.queryablestate.client.state.ImmutableListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import java.text.SimpleDateFormat


object _22_State_api_demo {
  val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointStorage("file:///Users/wangsheying/IdeaProjects/flink-demo-1.16/data/ck")
    env.setParallelism(1)
    println(env.getRestartStrategy)
    val source=env.socketTextStream("localhost",9999)
    .filter(_.nonEmpty).map(new MyMap)
      .print()
    env.execute()
  }

  class MyMap extends RichMapFunction[String,String] with CheckpointedFunction{
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tag=new ListStateDescriptor("tag",classOf[String])
    var  s: ListState[String]=null
    override def map(value: String): String = {
     s.add(value)
      val x=s.get().asScala.mkString("")

      if(value=="x") {
        Thread.sleep(2000)
        throw new Exception("崩掉")
      }
      x
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      println("-----snapshotState----------")
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      println(s"-------initializeState-----------")
      s=context.getOperatorStateStore.getListState(tag)
    }
  }


  class MyMap1 extends RichMapFunction[String,String]{
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tag=new ListStateDescriptor("tag",classOf[String])
    var  s: ListState[String]=null

    override def open(parameters: Configuration): Unit = {
      println("----open--------")
    }

    override def map(value: String): String = {
      println("----map----------")
      s.add(value)
      val x=s.get().asScala.mkString("")
      if(value=="x") throw new Exception("崩掉")
      x
    }

     def snapshotState(context: FunctionSnapshotContext): Unit = {
      println("-----snapshotState----------")
    }

     def initializeState(context: FunctionInitializationContext): Unit = {
      println(s"-------initializeState-----------")
      s=context.getOperatorStateStore.getListState(tag)
    }
  }

}

