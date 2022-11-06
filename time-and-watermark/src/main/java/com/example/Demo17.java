package com.example;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

/**
 * 自定义窗口
 */
public class Demo17 {
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
        streamOperator.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ClickEvent>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.getDatetime()))
                .keyBy(e -> e.getKey())
                .window(new MyWindowAssigner(5))
                // countTrigger触发时不会清理窗口
                .trigger(CountTrigger.of(2))
                // evictor使得窗口在计算sum前，只保留最后3个元素
                .evictor(CountEvictor.of(3, false))
                .sum("value")
                .print("out:");

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static class MyWindowAssigner extends WindowAssigner<Object, TimeWindow> {

        private int size;

        public MyWindowAssigner(int size) {
            this.size = size;
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
            long start = getWindowStart(timestamp);
            TimeWindow window = new TimeWindow(start, start + size);
            System.out.println("[assignWindows]elem:" + element + ", window:[" + window.getStart() + "," + window.getEnd() + ")");
            return Collections.singleton(window);
        }

        private long getWindowStart(long timestamp) {
            return timestamp - timestamp % size;
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }
}
