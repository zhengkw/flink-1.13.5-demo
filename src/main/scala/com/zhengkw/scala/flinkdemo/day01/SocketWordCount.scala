package com.zhengkw.scala.flinkdemo.day01

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

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
    def main(args: Array[String]) {
      // val env = StreamExecutionEnvironment.getExecutionEnvironment
      val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
      val text = env.socketTextStream("192.168.1.91", 9999)
      val wc = text.flatMap(_.split(" "))
        .map((_, 1))
        .keyBy(_._1)
        .sum(1)
      //打印控制台
      wc.print()
      env.execute("Window Stream WordCount")
    }
  }
}
