package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        DataStreamSink<Tuple2<String, Long>> sink = source.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] split = value.split(",");
                        Tuple2 tp = Tuple2.of(split[0], Long.parseLong(split[1]));
                        System.out.println("input:" + tp);
                        return tp;
                    }
                }).keyBy(e -> e.f0)
                .map(new MyMapFunc())
                .print("out:");

        System.out.println(env.getExecutionPlan());
        env.execute();

    }

    private static class MyMapFunc extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private ValueState<Long> state;
        private long sum = 0;
        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> tp) throws Exception {
            if(state.value() != null) {
                sum = state.value() + tp.f1;
            } else {
                sum = tp.f1;
            }
            state.update(sum);
            return Tuple2.of(tp.f0, sum);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> stateDesc = new ValueStateDescriptor<>("mySumState",
                    TypeInformation.of(new TypeHint<Long>() {
            }));
            state = getRuntimeContext().getState(stateDesc);
        }
    }
}
