package org.example

import example.avro.Student
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.ParquetWriterFactory
import org.apache.flink.formats.parquet.avro.{AvroParquetWriters, ParquetAvroWriters}
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf.Configuration
import java.time.Duration
import scala.util.Random

object _08_SinkOperator_Demo {
  case class MyStudent(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.enableCheckpointing(1000 * 5)

    val output = "data/file005"

    val writeFactoryArvo = AvroParquetWriters.forSpecificRecord(classOf[Student])
    // 使用反射,更加简单,其他不变
    val writeFactory = AvroParquetWriters.forReflectRecord(classOf[MyStudent])
    val sink = FileSink.forBulkFormat(new Path(output), writeFactory)
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build())
      .withBucketAssigner(new DateTimeBucketAssigner[MyStudent]())
      .withBucketCheckInterval(5)
      .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("pre").withPartSuffix("suf").build())
      .build()

    env.addSource(new MySourceFunction)
      .sinkTo(sink)
    env.execute()
  }

  class MySourceFunctionArvo extends SourceFunction[Student] {
    override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
      while (true) {
        ctx.collect(Student.newBuilder().setName("li").setAge(Random.nextInt(100)).build())
      }
    }

    override def cancel(): Unit = {}
  }


  class MySourceFunction extends SourceFunction[MyStudent] {
    override def run(ctx: SourceFunction.SourceContext[MyStudent]): Unit = {
      while (true) {
        ctx.collect(MyStudent("a", 1))
      }
    }

    override def cancel(): Unit = {}
  }
}
