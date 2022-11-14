package org.example

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._


object _25_StateTTL_keyby_api_demo {
  val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()


    env.setStateBackend(new EmbeddedRocksDBStateBackend())
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointStorage("file:///Users/wangsheying/IdeaProjects/flink-demo-1.16/data/ck")
    env.setParallelism(1)
    println(env.getRestartStrategy)
    val source=env.socketTextStream("localhost",9999)
    .filter(_.nonEmpty).keyBy(_.length).map(new MyMap)
      .print()
    env.execute()
  }

  class MyMap extends RichMapFunction[String,String]{
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ttlConfig=StateTtlConfig.newBuilder(Time.seconds(10))
      .updateTtlOnCreateAndWrite()
      .useProcessingTime()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()
    val tag=new ListStateDescriptor("tag",classOf[String])
    var  s: ListState[String]=null
    val tagv=new ValueStateDescriptor("tagv",classOf[String])
    var v: ValueState[String]=null


    override def open(parameters: Configuration): Unit = {
      println("-------open---------")
      tagv.enableTimeToLive(ttlConfig)
        v= getRuntimeContext.getState(tagv)

    }
    override def map(value: String): String = {
      println(s"-------map-----${new Date()}----")
      v.update(v.value()+value)
      v.value()
    }
  }


}

