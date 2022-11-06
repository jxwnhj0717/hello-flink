package com.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;

public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Tuple3[] inputs = {
                Tuple3.of("user1", 1000L, 1),
                Tuple3.of("user1", 1999L, 2),
                Tuple3.of("user1", 2000L, 3),
                Tuple3.of("user1", 2100L, 4),
                Tuple3.of("user1", 2130L, 5)
        };
        DataStreamSource<Tuple3<String, Long, Integer>> source = env.addSource(
                new SourceFunction<Tuple3<String, Long, Integer>>() {

            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                Arrays.asList(inputs).forEach(tp3 -> {
                    System.out.println("collectWithTimestamp:" + tp3.f1);
                    ctx.collectWithTimestamp(tp3, (long) tp3.f1);
                    System.out.println("emitWatermark:" + ((long) tp3.f1 - 1));
                    ctx.emitWatermark(new Watermark((long) tp3.f1 - 1));
                });
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {

            }
        });

        source.print();
        env.execute();

    }
}
