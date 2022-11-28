package com.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class ExecuteSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        String path = "/Users/huangjin/IdeaProjects/flink/table/src/main/resources/data.csv";
        String ddl = "create table mytable (" +
                " id int," +
                " cname string," +
                " num int," +
                " addtime timestamp(3)," +
                " watermark for addtime as addtime - interval '0.01' second" +
                " ) with (" +
                " 'connector.type' = 'filesystem'," +
                " 'connector.path' = '" + path + "'," +
                " 'format.type' = 'csv'" +
                " )";
        TableResult result = tenv.executeSql(ddl);
        result.print();

        TableResult result2 = tenv.from("mytable")
                .select($("*"))
                .execute();
        result2.print();

        Table table = tenv.from("mytable");

    }
}
