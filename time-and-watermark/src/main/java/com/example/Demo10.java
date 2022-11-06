package com.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 无迟到的乱序数据处理
 */
public class Demo10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(3000L);
        env.setParallelism(1);
        DataStreamSource<ClickEvent> mySource = env.fromElements(
                new ClickEvent("user1", 2L, 2),
                new ClickEvent("user2", 3L, 3),
                new ClickEvent("user1", 1L, 1),
                new ClickEvent("user2", 7L, 7),
                new ClickEvent("user1", 3L, 3),
                new ClickEvent("user1", 5L, 5),
                new ClickEvent("user1", 9L, 9),
                new ClickEvent("user1", 6L, 6),
                new ClickEvent("user1", 12L, 12),
                new ClickEvent("user1", 17L, 17),
                new ClickEvent("user1", 10L, 10),
                new ClickEvent("user1", 16L, 16),
                new ClickEvent("user1", 19L, 19),
                new ClickEvent("user1", 11L, 11),
                new ClickEvent("user1", 18L, 18),
                new ClickEvent("user1", 13L, 13),
                new ClickEvent("user1", 20L, 20)
        );

        mySource.assignTimestampsAndWatermarks(((WatermarkStrategy<ClickEvent>)
                context -> new PunctuatedAssigner())
                .withTimestampAssigner((SerializableTimestampAssigner<ClickEvent>)
                (element, recordTimestamp) -> element.getDatetime()))
                .keyBy(f -> f.getKey())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(4)))
                .process(new MyProcessWindowFunction())
                .print();
        System.out.println(env.getExecutionPlan());
        env.execute();

    }

    private static class PunctuatedAssigner implements WatermarkGenerator<ClickEvent> {

        private int size = 0;

        @Override
        public void onEvent(ClickEvent event, long eventTimestamp, WatermarkOutput output) {
            size++;
            System.out.println("EventTime:"+event.getDatetime()+" ");
            if(event.getDatetime() == 3 && size ==5) {
                output.emitWatermark(new Watermark(4L));
                System.out.println("Watermark: 4");
            } else if(event.getDatetime() == 6) {
                output.emitWatermark(new Watermark(9L));
                System.out.println("Watermark: 9");
            } else if(event.getDatetime() == 11) {
                output.emitWatermark(new Watermark(11L));
                System.out.println("Watermark: 11");
            } else if(event.getDatetime() == 13) {
                output.emitWatermark(new Watermark(13L));
                System.out.println("Watermark: 13");
            } else if(size == 17) {
                output.emitWatermark(new Watermark(Long.MAX_VALUE));
                System.out.println("Watermark: " + Long.MAX_VALUE);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<ClickEvent, ClickEvent, String, TimeWindow> {

        // 窗口结束时触发
        @Override
        public void process(String s,
                            ProcessWindowFunction<ClickEvent, ClickEvent, String, TimeWindow>.Context context,
                            Iterable<ClickEvent> elements,
                            Collector<ClickEvent> out) throws Exception {
            String msg = String.format("[WindowProcess] key:%s, watermark:%s, window.start:%d, window.end:%d, timestamp:%d",
                    s, context.currentWatermark(), context.window().getStart(), context.window().getEnd(),
                    context.window().maxTimestamp());
            System.out.println(msg);
            List<ClickEvent> list = StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(elements.iterator(), Spliterator.ORDERED), false)
                    .collect(Collectors.toList());
            list.sort((o1, o2) -> (int) (o1.getDatetime() - o2.getDatetime()));
            System.out.println("-------Window ClickEvents-------");
            list.forEach(e -> System.out.println(e));

        }
    }
}
