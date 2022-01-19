package com.zhengkw.scala.flinkdemo.day01

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:SocketUseFunctionWC
 * @author: zhengkw
 * @description:
 * @date: 22/01/19下午 5:41
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SocketUseFunctionWC {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(6)
    val text = env.socketTextStream("loyx04", 9999)
    val value1 = text.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        val strings = value.split(" ")
        for (s <- strings) {
          //发送到下级
          out.collect(s)
        }
      }
    }).map((_, 1)).keyBy(_._1).sum(1)
    value1.print()

    env.execute("Function Proessed Stream WordCount")
  }
}
