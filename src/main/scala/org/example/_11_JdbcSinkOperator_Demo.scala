package org.example
import com.google.gson.Gson
import com.mysql.cj.jdbc.MysqlXADataSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.function.SerializableSupplier

import java.sql.PreparedStatement
import javax.sql.XADataSource

object _11_JdbcSinkOperator_Demo {
  case class Student11(name:String,age:Int)
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.enableCheckpointing(1000 * 5)

    val sink=JdbcSink.sink(
      "INSERT INTO runoob_tbl (runoob_title, runoob_author) VALUES (?,?)",
      new JdbcStatementBuilder[Student11] {
        override def accept(t: PreparedStatement, u: Student11): Unit = {
          t.setString(1,u.name)
          t.setInt(2,u.age)
        }
      }
      ,
      JdbcExecutionOptions.builder().withBatchIntervalMs(1000).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://localhost:3306/RUNOOB")
        .withUsername("root")
        .withPassword("123456")
        .build()
    )


    val sinkExactlyOnce=JdbcSink.exactlyOnceSink(
      "INSERT INTO runoob_tbl (runoob_title, runoob_author) VALUES (?,?)",
      new JdbcStatementBuilder[Student11] {
        override def accept(t: PreparedStatement, u: Student11): Unit = {
          t.setString(1,u.name)
          t.setInt(2,u.age)
        }
      },
      JdbcExecutionOptions.builder().withBatchSize(1).withMaxRetries(0).build(),
      JdbcExactlyOnceOptions.builder()
        .withTransactionPerConnection(true)
        .build(),
      new SerializableSupplier[XADataSource] {
        override def get(): XADataSource = {
          val x=new MysqlXADataSource()
          x.setUrl("jdbc:mysql://localhost:3306/RUNOOB")
           x.setUser("root")
            x.setPassword("123456")
          x
        }
      }
    )

    val a=env.addSource(new SourceFunction[Student11] {
      override def run(ctx: SourceFunction.SourceContext[Student11]): Unit = {
        var i=0
        while(true){
          i+=1
          Thread.sleep(1000)
          ctx.collect(Student11(s"exactly${i}",i%1000))
        }
      }

      override def cancel(): Unit = {}
    })
      a.print()
      a.addSink(sinkExactlyOnce)



    env.execute()


  }

}
