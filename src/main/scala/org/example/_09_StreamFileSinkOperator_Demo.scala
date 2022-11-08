package org.example

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object _09_StreamFileSinkOperator_Demo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointStorage("file://data/ck")


    val schema = SchemaBuilder.builder()
      .record("DataRecord")
      .namespace("com.wsy")
      .doc("文档注释")
      .fields()
      .requiredString("name")
      .requiredInt("age")
      .endRecord()

    val record = new GenericData.Record(schema)
    record.put("name", "a")
    record.put("age",1)
    println(record)

    val writer = AvroParquetWriters.forGenericRecord(schema)
    val sink = FileSink.forBulkFormat(new Path("data/file006"), writer)
      .withBucketAssigner(new DateTimeBucketAssigner)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix("par").build())
      .build()

    TypeInformation.of(classOf[GenericRecord])


    env.addSource(new MySourceFunction)
      .map(new MyMapFunction)
      .sinkTo(sink)

    env.execute()

  }


  class MySourceFunction extends SourceFunction[Student001] {
    override def run(ctx: SourceFunction.SourceContext[Student001]): Unit = {
      while (true) {
        ctx.collect(Student001("a", 1))
      }
    }

    override def cancel(): Unit = {}
  }

}

case class Student001(name: String, age: Int)


class MyMapFunction extends RichMapFunction[Student001,GenericRecord]{
  val schema = SchemaBuilder.builder()
    .record("DataRecord")
    .namespace("com.wsy")
    .doc("文档注释")
    .fields()
    .requiredString("name")
    .requiredInt("age")
    .endRecord()

  override def map(value: Student001): GenericRecord = {
    val record: GenericData.Record = new GenericData.Record(schema)
    record.put("name", value.name)
    record.put("age", value.age)
    record
  }
}


