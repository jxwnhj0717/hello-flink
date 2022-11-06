package com.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10);
        env.setParallelism(1);

        DataStreamSource<ClickEvent> source = env.fromElements(
                new ClickEvent("user1", 1L, 1),
                new ClickEvent("user1", 2L, 2),
                new ClickEvent("user1", 4L, 3),
                new ClickEvent("user1", 3L, 4),
                new ClickEvent("user1", 5L, 5),
                new ClickEvent("user1", 6L, 6),
                new ClickEvent("user1", 7L, 7),
                new ClickEvent("user1", 8L, 8)
        );

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        SingleOutputStreamOperator<ClickEvent> streamOperator = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ClickEvent>() {
            @Override
            public long extractAscendingTimestamp(ClickEvent element) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    ;
                }
                System.out.println("getDatetime:" + sdf.format(new Date()) + "->" + element.getDatetime());
                return element.getDatetime();
            }
        });

        streamOperator.print();
        env.execute();

    }
}
