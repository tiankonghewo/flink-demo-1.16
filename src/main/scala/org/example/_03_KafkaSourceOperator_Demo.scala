package org.example

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.{FixedDelayRestartStrategyConfiguration, RestartStrategyConfiguration}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.util.concurrent.TimeUnit

object _03_KafkaSourceOperator_Demo {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    //env.enableCheckpointing(1000*50)
    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, Time.of(5, TimeUnit.SECONDS)
    ))

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("quickstart-events2")
      .setGroupId("my-group5")
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .setProperty("enable.auto.commit","true")
      .setProperty("auto.commit.interval.ms","10000")
      .build()


    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String](), "Kafka Source")
      .map{
        x=>
          //if(x=="4") throw new Exception("中断")
          x
      }.disableChaining().print()
    env.execute()

  }
}
