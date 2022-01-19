package com.zhengkw.scala.flinkdemo.day01


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName:SocketWordCount
 * @author: zhengkw
 * @description:
 * 使用 shell 命令发socket  nc -lk port
 * @date: 22/01/19下午 4:15
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    val text = env.socketTextStream("loyx04", 9999)
    val counts = text.flatMap {
      _.toLowerCase.split(" ") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")

  }
}
