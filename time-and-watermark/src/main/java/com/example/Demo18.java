package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 增量聚合
 */
public class Demo18 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<ClickEvent> streamOperator = source.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String value, Collector<ClickEvent> out) throws Exception {
                String[] split = value.split(",");
                ClickEvent event = new ClickEvent(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                System.out.println("input:" + event);
                out.collect(event);
            }
        });
        streamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> e.getDatetime()))
                .keyBy(e -> e.getKey())
                .countWindow(3)
                .reduce(new MyReduceFunction())
                .print("out:");
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static class MyReduceFunction implements ReduceFunction<ClickEvent> {
        @Override
        public ClickEvent reduce(ClickEvent e1, ClickEvent e2) throws Exception {
            ClickEvent event = new ClickEvent(e1.getKey(), e1.getDatetime(), e1.getValue() + e2.getValue());
            System.out.println("e1:" + e1 + ", e2:" + e2 + "， reduce:" + event);
            return event;
        }
    }
}
