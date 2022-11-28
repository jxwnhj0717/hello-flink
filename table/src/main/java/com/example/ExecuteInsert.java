package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class ExecuteInsert {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String sourceDDL = "create table table_gen (" +
                " name string," +
                " age int" +
                " ) with (" +
                " 'connector' = 'datagen'," +
                " 'rows-per-second' = '10'," +
                " 'fields.age.kind' = 'sequence'," +
                " 'fields.age.start' = '1'," +
                " 'fields.age.end' = '100'," +
                " 'fields.name.length'='1'" +
                " )";
        String sinkDDL = "create table table_print (" +
                " name string," +
                " age_avg decimal(10, 2)" +
                " ) with (" +
                " 'connector' = 'print'" +
                " )";
        tenv.executeSql(sourceDDL);
        tenv.executeSql(sinkDDL);

//        tenv.from("table_gen")
//                .select($("*"))
//                .execute()
//                .print();
        tenv.from("table_gen")
                .groupBy($("name"))
                .select($("name"),
                        $("age").avg().as("age_avg"))
                //.execute().print();
                .executeInsert("table_print", false);


    }
}
