package org.example
import com.google.gson.Gson
import com.mysql.cj.jdbc.MysqlXADataSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.util.function.SerializableSupplier

object _13_SideOutput_Demo {
  case class Student13(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    //env.enableCheckpointing(1000 * 5)
    val source=env.addSource(new SourceFunction[Student13] {
      override def run(ctx: SourceFunction.SourceContext[Student13]): Unit = {
        var i=0
        while(true){
          i+=1
          Thread.sleep(100)
          ctx.collect(Student13(s"exactly${i}",i%2))
        }
      }

      override def cancel(): Unit = {}
    })

    val mainStream=source.process(
      new ProcessFunction[Student13,Student13] {
        override def processElement(value: Student13, ctx: ProcessFunction[Student13, Student13]#Context, out: Collector[Student13]): Unit = {
          if(value.age==0){
            out.collect(value)
          }else{
            ctx.output(new OutputTag[Student13]("m"),value)
          }
        }
      }
    )

    //mainStream.print()
    val sideStream=mainStream.getSideOutput(new OutputTag[Student13]("m"))
    sideStream.print()
    env.execute()

  }

}
