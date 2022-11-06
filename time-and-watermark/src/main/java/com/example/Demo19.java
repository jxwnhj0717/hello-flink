package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 全量聚合
 */
public class Demo19 {
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
                .process(new MyWindowFunc())
                .print("out:");

        System.out.println(env.getExecutionPlan());
        env.execute();

    }

    private static class MyWindowFunc extends ProcessWindowFunction<ClickEvent, Tuple2<String, Double>, String, GlobalWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<ClickEvent, Tuple2<String, Double>, String, GlobalWindow>.Context context, Iterable<ClickEvent> elements, Collector<Tuple2<String, Double>> out) throws Exception {
            long[] sum = new long[1];
            int[] count =  new int[0];
            String key = elements.iterator().next().getKey();
            elements.forEach(e -> {
                sum[0] += e.getValue();
                count[0]++;
            });
            out.collect(Tuple2.of(key, (double) sum[0] / count[0]));
        }
    }
}
