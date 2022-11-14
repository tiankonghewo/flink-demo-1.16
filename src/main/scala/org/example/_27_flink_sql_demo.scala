package org.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

import java.text.SimpleDateFormat


object _27_flink_sql_demo {
  val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.executeSql(
      """
        |CREATE TABLE KafkaTable (
        |  `name` STRING,
        |  `age` BIGINT
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'flink_sql_demo001',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup11',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin
    )

    tableEnv.executeSql(
      """
        |select name, sum(age) as he from KafkaTable group by name
        |""".stripMargin).print()

//    val t=tableEnv.sqlQuery(
//      """
//        |select name, sum(age) as he from KafkaTable group by name
//        |""".stripMargin)

//    tableEnv.toChangelogStream(t).print()
//
//
//
//    env.execute()

  }


}

