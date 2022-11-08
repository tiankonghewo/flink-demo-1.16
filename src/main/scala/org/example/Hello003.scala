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
import org.apache.flink.streaming.api.functions.source.{FileProcessingMode, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.formats.parquet.avro.{AvroParquetWriters, ParquetAvroWriters}
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.parquet.hadoop.ParquetWriter

import java.time.Duration


object Hello003 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.enableCheckpointing(1000*5)

    val outputPath="data/file003"
    val sink: FileSink[String] = FileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(Duration.ofSeconds(10))
          .withInactivityInterval(Duration.ofSeconds(10))
          .withMaxPartSize(MemorySize.ofMebiBytes(5*1024))
          .build())
      .withBucketAssigner(new DateTimeBucketAssigner[String]())
      .withBucketCheckInterval(5)
      .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("pre").withPartSuffix("suf").build())
      .build()




    env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true){
          ctx.collect("a")
        }
      }

      override def cancel(): Unit = {}
    })
      .sinkTo(sink)
    env.execute()

  }

}
