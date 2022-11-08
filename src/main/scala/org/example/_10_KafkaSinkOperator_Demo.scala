package org.example

import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object _10_KafkaSinkOperator_Demo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //env.setParallelism(1)
    //env.enableCheckpointing(1000 * 5)



    val sink=KafkaSink.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("topic001")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setTransactionalIdPrefix("wsy-")
      .build()


   val source= env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true){
          Thread.sleep(1000)
          ctx.collect("a")
        }
      }

      override def cancel(): Unit = {}
    })

     //source.print()
     source .sinkTo(sink)



    env.execute()

  }
}
