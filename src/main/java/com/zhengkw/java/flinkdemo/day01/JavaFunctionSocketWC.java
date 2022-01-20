package com.zhengkw.java.flinkdemo.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName:JavaFunctionSocketWC
 * @author: zhengkw
 * @description: 实现function写法
 * @date: 22/01/20下午 2:49
 * @version:1.0
 * @since: jdk 1.8
 */
public class JavaFunctionSocketWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(6);
        DataStreamSource<String> socket = env.socketTextStream("loyx04", 9999, '\n');

        //Function 写法
        SingleOutputStreamOperator<String> flatMap = socket.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arrays = value.split(" ");
                for (String word : arrays) {
                    out.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
       // SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy("f0").sum(1);
        sum.print();
        env.execute();
    }
}
