package com.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 滚动事件时间窗口
 */
public class TumblingEventTimeWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> streamOperator = source.map(new MapFunction<String, Tuple3<String, Long, Long>>() {
            @Override
            public Tuple3<String, Long, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, Long>>forMonotonousTimestamps().withTimestampAssigner(
                        (SerializableTimestampAssigner<Tuple3<String, Long, Long>>)
                                (element, recordTimestamp) -> element.f1
                )
        );

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        Table table = tenv.fromDataStream(streamOperator,
                $("f0").as("key"),
                $("f1").rowtime().as("rs"),
                $("f2").as("ivalue"));
        table.printSchema();

        TumbleWithSizeOnTimeWithAlias window = Tumble.over(lit(3L).milli()).on($("rs")).as("w");
        Table select = table.window(window)
                .groupBy($("w"), $("key")) //
                .select($("key"),
                        $("w").start().as("wstart"),
                        $("w").end().as("wend"),
                        $("w").rowtime().as("wrs"),
                        $("ivalue").sum().as(("isum")));
        select.printSchema();
        select.execute().print();


    }
}
