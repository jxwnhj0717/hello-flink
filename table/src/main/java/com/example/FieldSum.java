package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;

public class FieldSum {
    public static void main(String[] args) {
        String path = "/Users/huangjin/IdeaProjects/flink/table/src/main/resources/field_sum";
        File file = new File(path);
        if(file.exists()) {
            for (File listFile : file.listFiles()) {
                listFile.delete();
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String ddl = "create table my_table (" +
                " id int," +
                " name string," +
                " amount int" +
                " ) with (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'file://" + path + "'," +
                " 'format' = 'csv'" + // csv format需要加依赖
                " )";
        tenv.executeSql(ddl);

        tenv.executeSql("insert into my_table values(1, 'apple', 5)");
        tenv.executeSql("insert into my_table values(2, 'banana', 5)");
        tenv.executeSql("insert into my_table values(3, 'orange', 5)");
        tenv.executeSql("insert into my_table values(4, 'apple', 3)");

        Table table = tenv.from("my_table");
        table.select($("*")).execute().print();

        tenv.executeSql("select name, sum(amount) from my_table group by name").print();



    }


}
