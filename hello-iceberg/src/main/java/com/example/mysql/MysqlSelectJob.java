package com.example.mysql;

import com.example.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ExternalSchemaTranslator;
import org.apache.flink.table.connector.ChangelogMode;

public class MysqlSelectJob {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        //config.set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().addConfiguration(config);

        String sql = FileUtils.readFile("mysql_table.sql");
        tenv.executeSql(sql);
		Table table = tenv.sqlQuery("SELECT * FROM mysql_table" );
        // No Ranking Output Optimization 不要在外层select rownum
//        Table table = tenv.sqlQuery("select id, name, audit, update_time from (" +
//                " select *," +
//                " row_number() over (partition by id order by update_time desc) as rownum" +
//                " from mysql_table)" +
//                " where rownum = 1");

       tenv.toChangelogStream(table)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
//        // 创建下游数据表，这里使用print类型的connector，将数据直接打印出来
//        tenv.executeSql("CREATE TABLE sink_table (id INT NOT NULL, name STRING, age INT, update_time TIMESTAMP(3)) WITH ('connector' = 'print')");
//        // 将CDC数据源和下游数据表对接起来
//        tenv.executeSql("INSERT INTO sink_table SELECT id, name, age, update_time FROM mysql_table");


    }
}
