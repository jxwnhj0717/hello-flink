package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1L);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator =
                source.flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        String[] group = value.split(",");
                        out.collect(Tuple3.of(group[0], group[1], Long.parseLong(group[2])));
                    }
                }).setParallelism(2)
                .partitionCustom(new Partitioner<String>() {
                    @Override
                    public int partition(String key, int numPartitions) {
                        if (key.equals("a")) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }
                }, f -> f.f1)
                .assignTimestampsAndWatermarks(new MyTimeAssigner())
                .setParallelism(2);
        streamOperator
                .keyBy(f -> f.f2) // keyBy作用？？
                .process(new ProcessFunction<Tuple3<String, String, Long>, Object>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> value, ProcessFunction<Tuple3<String, String, Long>, Object>.Context ctx, Collector<Object> out) throws Exception {
                        long tid = Thread.currentThread().getId();
                        String tname = Thread.currentThread().getName();
                        String format = String.format("[ProcessElement1] threadId:%d, threadName:%s, elem:%s, watermark:%s",
                                tid, tname, value, ctx.timerService().currentWatermark());
                        System.out.println(format);
                        out.collect(value);
                    }
                })
//                .setParallelism(2) //
//                .process(new ProcessFunction<Object, Object>() {
//                    @Override
//                    public void processElement(Object value, ProcessFunction<Object, Object>.Context ctx, Collector<Object> out) throws Exception {
//                        long tid = Thread.currentThread().getId();
//                        String tname = Thread.currentThread().getName();
//                        String format = String.format("[ProcessElement2] threadId:%d, threadName:%s, elem:%s, watermark:%s",
//                                tid, tname, value, ctx.timerService().currentWatermark());
//                        System.out.println(format);
//                        out.collect(value);
//                    }
//                })
                .setParallelism(1) //
                .print() //
                .setParallelism(1);

        System.out.println("plan:" + env.getExecutionPlan());
        env.execute();

    }

    private static class MyTimeAssigner implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>> {

        private long maxTimestamp = 0L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
            maxTimestamp = Math.max(element.f2, maxTimestamp);
            long tid = Thread.currentThread().getId();
            String tname = Thread.currentThread().getName();
            String format = String.format("[extractTimestamp]threadId:%d, threadName:%s, elem:%s, watermark:%s",
                    tid, tname, element, getCurrentWatermark().getTimestamp());
            System.out.println(format);
            return element.f2;
        }
    }
}
