package com.zhengkw.java.flinkdemo.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @ClassName:JavaLambdaSocketWC
 * @author: zhengkw
 * @description:
 * @date: 22/01/20下午 2:27
 * @version:1.0
 * @since: jdk 1.8
 */
public class JavaLambdaSocketWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(6);
        DataStreamSource<String> socket = env.socketTextStream("loyx04", 9999, '\n');
        //lambda表达式写方法实现，前提是该接口只有一个方法！ FlatMapFunction
        SingleOutputStreamOperator<String> flatMap = socket.flatMap((String value, Collector<String> out) -> {
            Arrays.stream(value.split(" ")).forEach(word -> {
                out.collect(word);
            });
        }).returns(Types.STRING);
        //stringSingleOutputStreamOperator.print();
        //转为Tuple2类型，需要returns Types给到Flink。
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
        sum.print();
        env.execute();
    }
}
