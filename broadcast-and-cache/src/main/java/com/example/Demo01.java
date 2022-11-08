package com.example;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class Demo01 {
    private static final MapStateDescriptor<String, String> mapStateDesc =
            new MapStateDescriptor<String, String>("sql inject",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        List<String> strings = Arrays.asList("delete", "drop", "truncate", "alert");
        BroadcastStream<String> broadcast = env.fromCollection(strings).broadcast(mapStateDesc);
        source.connect(broadcast)
                .process(new MyBroadcastProcessFunc())
                .print("out:");
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static class MyBroadcastProcessFunc extends BroadcastProcessFunction<String, String, String> {

        @Override
        public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
            System.out.println("processElement:" + value);
            ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDesc);
            String[] split = value.split(" ");
            for (String word : split) {
                if(broadcastState.contains(word)) {
                    System.out.println("sql inject:" + value);
                    out.collect(value);
                    break;
                }
            }
        }

        @Override
        public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
            System.out.println("processBroadcastElement:" + value);
            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDesc);
            broadcastState.put(value, value);
        }
    }
}
