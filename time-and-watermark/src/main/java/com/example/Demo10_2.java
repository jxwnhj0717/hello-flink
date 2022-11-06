package com.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 有迟到的乱序数据处理
 */
public class Demo10_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(3000L);
        env.setParallelism(1);
        DataStreamSource<ClickEvent> mySource = env.fromElements(
                new ClickEvent("user1", 2L, 2),
                new ClickEvent("user1", 3L, 3),
                new ClickEvent("user1", 1L, 1),
                new ClickEvent("user1", 7L, 7),
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
                new ClickEvent("user1", 5L, 0), // late
                new ClickEvent("user1", 10L, 0), // late
                new ClickEvent("user1", 18L, 18),
                new ClickEvent("user1", 13L, 13),
                new ClickEvent("user1", 20L, 20)
        );

        OutputTag<ClickEvent> outputTag = new OutputTag<ClickEvent>("myLater") {
        };

        SingleOutputStreamOperator<ClickEvent> streamOperator = mySource
                .assignTimestampsAndWatermarks(new WatermarkStrategy<ClickEvent>() {
                    @Override
                    public WatermarkGenerator<ClickEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new PunctuatedAssigner();
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                    @Override
                    public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                        return element.getDatetime();
                    }
                }))
                .keyBy(e -> e.getKey())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(4L)))
                .allowedLateness(Time.milliseconds(2L))
                .sideOutputLateData(outputTag)
                .process(new MyProcessWindowFunction());

        streamOperator.print();
        streamOperator.getSideOutput(outputTag).process(new ProcessFunction<ClickEvent, Object>() {
            @Override
            public void processElement(ClickEvent value, ProcessFunction<ClickEvent, Object>.Context ctx, Collector<Object> out) throws Exception {
                System.out.println("LateEvent: " + value);
            }
        });

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
                // 这个水位和窗口的allowedLateness决定了延迟数据是重新触发窗口，还是sideOutput旁路
                output.emitWatermark(new Watermark(12L));
                System.out.println("Watermark: 11");
            } else if(event.getDatetime() == 13) {
                output.emitWatermark(new Watermark(13L));
                System.out.println("Watermark: 13");
            } else if(size == 19) {
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
