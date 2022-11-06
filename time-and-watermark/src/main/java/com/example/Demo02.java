package com.example;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10);
        env.setParallelism(1);
        DataStreamSource<ClickEvent> source = env.fromElements(
                new ClickEvent("user1", 1L, 1),
                new ClickEvent("user1", 2L, 2),
                new ClickEvent("user1", 3L, 3),
                new ClickEvent("user1", 4L, 4),
                new ClickEvent("user1", 5L, 5),
                new ClickEvent("user1", 6L, 6),
                new ClickEvent("user1", 7L, 7),
                new ClickEvent("user1", 8L, 8)
        );

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        SingleOutputStreamOperator<ClickEvent> streamOperator =
                source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickEvent>() {
            private long maxTimestamp = 0L;
            private long delay = 0L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                System.out.println("--onPeriodicEmit:" + sdf.format(new Date()) + "->" + (maxTimestamp - delay));
                return new Watermark(maxTimestamp - delay);
            }

            @Override
            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    ;
                }
                maxTimestamp = Math.max(element.getDatetime(), maxTimestamp);
                System.out.println("extractTimestamp:" + sdf.format(new Date()));
                return element.getDatetime();
            }
        });
        streamOperator.print();
        System.out.println("start:" + sdf.format(new Date()));
        env.execute();
    }

}
