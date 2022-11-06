package com.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滚动窗口 TumblingEventTimeWindows
 *
 * input:ClickEvent(key=a, datetime=1, value=1)
 * input:ClickEvent(key=a, datetime=2, value=2)
 * input:ClickEvent(key=a, datetime=3, value=3) //触发窗口[0,3)，是所有key，不是key=a
 * sum:> ClickEvent(key=a, datetime=1, value=3)
 * input:ClickEvent(key=b, datetime=1, value=2) //延迟数据，丢弃
 * input:ClickEvent(key=b, datetime=3, value=4)
 * input:ClickEvent(key=b, datetime=5, value=6)
 * input:ClickEvent(key=b, datetime=7, value=8) //触发窗口[3,6)
 * sum:> ClickEvent(key=a, datetime=3, value=3)
 * sum:> ClickEvent(key=b, datetime=3, value=10)
 *
 */
public class Demo12 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<ClickEvent> streamOperator = source.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String value, Collector<ClickEvent> out) throws Exception {
                String[] split = value.split(",");
                ClickEvent clickEvent = new ClickEvent(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                System.out.println("input:" + clickEvent);
                out.collect(clickEvent);
            }
        });
        streamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<ClickEvent>)
                                        (element, recordTimestamp) -> element.getDatetime()
                        )
        ).keyBy(e -> e.getKey())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(3L)))
                .sum("value")
                .print("sum:");

        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
