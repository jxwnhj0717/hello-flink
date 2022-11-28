package com.example.kafka;

import com.example.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * upsert kafka 查询不到数据，why？
 */
public class KafkaSelectJob2 {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        config.set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
        config.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().addConfiguration(config);

        String sql = FileUtils.readFile("kafka_table2.sql");
        tenv.executeSql(sql);
//        Table table = tenv.sqlQuery("select * from (" +
//                " select *," +
//                " row_number() over (partition by user_id order by ts desc) as rownum" +
//                " from kafka_table2)" +
//                " where rownum = 1");
        Table table = tenv.sqlQuery("select * from kafka_table2");
        table.execute().print();
//        TableResult result = table.execute();
//        CloseableIterator<Row> iter = result.collect();
//        int no = 1;
//        while(iter.hasNext()) {
//            System.out.println(no + ":" + iter.next());
//            no++;
//        }
    }
}
