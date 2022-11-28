package com.example.kafka;

import com.example.FileUtils;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;

public class KafkaSelectJob {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String sql = FileUtils.readFile("kafka_table.sql");
        tenv.executeSql(sql);
        //Table table = tenv.sqlQuery("select * from kafka_table");
        Table table = tenv.sqlQuery("select * from (" +
                " select *," +
                " row_number() over (partition by user_id order by ts desc) as rownum" +
                " from kafka_table)" +
                " where rownum = 1");
        table.execute().print();
    }
}
