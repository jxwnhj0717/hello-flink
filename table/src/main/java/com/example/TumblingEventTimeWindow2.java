package com.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 滚动事件时间窗口
 */
public class TumblingEventTimeWindow2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
        // TableResult resutl1 = table.execute();

        Table select = tenv.sqlQuery("select key," +
                " tumble_start(rs, interval '0.003' second) as wstart," +
                " tumble_end(rs, interval '0.003' second) as wend," +
                " tumble_rowtime(rs, interval '0.003' second) as wrs," +
                " sum(ivalue) as isum" +
                " from " + table +
                " group by tumble(rs, interval '0.003' second), key");

        select.printSchema();
        TableResult result2 = select.execute();


        while(true) {
//            if(resutl1.collect().hasNext()) {
//                System.out.println("table1:" + resutl1.collect().next());
//            }
            if(result2.collect().hasNext()) {
                System.out.println("table2:" + result2.collect().next());
            }
        }
    }
}
