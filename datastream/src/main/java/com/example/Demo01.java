package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromElements("flink", "spark", "flink");
        SingleOutputStreamOperator<Tuple2<String, Integer>> op =
                source.map(value -> Tuple2.of(value, 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT));
        op.print();

        env.execute();
    }
}
