package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

import javax.xml.crypto.Data;

import static org.apache.flink.table.api.Expressions.$;

public class CreateTempTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.connect(
                new FileSystem().path("/Users/huangjin/IdeaProjects/flink/table/src/main/resources/data.csv")
        ).withFormat(
                new OldCsv().fieldDelimiter(",")
        ).withSchema(
                new Schema()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("age", DataTypes.INT())
        ).inAppendMode()
                .createTemporaryTable("mytable");

        Table mytable = tenv.from("mytable");
        TableResult result = mytable.select($("*")).execute();
        result.print();

    }
}
