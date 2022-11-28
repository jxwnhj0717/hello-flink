package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        DataStreamSource<Tuple2<Integer, String>> source = env.fromElements(
                Tuple2.of(1, "hello"),
                Tuple2.of(1, "flow"),
                Tuple2.of(2, "flink"),
                Tuple2.of(1, "flink")
        );
        Table table = tenv.fromDataStream(source,
                $("f0").as("count"),
                $("f1").as("word"));
        table.printSchema();

        Table table2 = table
                .where($("word").like("f%"))
                .groupBy($("word"))
                .select($("word"), $("count").sum().as("allCount"));
        table2.printSchema();

        TableResult result = table2.execute();
        result.print();

        System.out.println(table.explain());
    }
}
