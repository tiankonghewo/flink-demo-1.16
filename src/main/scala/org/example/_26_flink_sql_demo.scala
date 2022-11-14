package org.example

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.scala._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._


object _26_flink_sql_demo {
  val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {


    val env = TableEnvironment.create(EnvironmentSettings.inStreamingMode())

    env.executeSql(
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

    env.executeSql(
      """
        |CREATE TABLE KafkaTable_sink (
        |  `name` STRING,
        |  `age` BIGINT
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'flink_sql_demo002',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'format' = 'json'
        |)
        |""".stripMargin
    )



    env.executeSql(
      """
        |INSERT INTO KafkaTable_sink SELECT name, age FROM KafkaTable
        |""".stripMargin
    )



  }


}

