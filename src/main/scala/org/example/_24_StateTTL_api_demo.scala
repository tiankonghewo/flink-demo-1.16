package org.example

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._


object _24_StateTTL_api_demo {
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
    val ttlConfig=StateTtlConfig.newBuilder(Time.seconds(10)).updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()
    val tag=new ListStateDescriptor("tag",classOf[String])
    var  s: ListState[String]=null

    override def open(parameters: Configuration): Unit = {
      println("-------open---------")
    }
    override def map(value: String): String = {
      println(s"-------map-----${new Date()}----")

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
      tag.enableTimeToLive(ttlConfig)
      s=context.getOperatorStateStore.getListState(tag)
    }
  }


}

