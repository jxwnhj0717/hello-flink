/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.iceberg;

import com.example.FileUtils;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ExternalSchemaTranslator;
import org.apache.flink.table.connector.ChangelogMode;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Scanner;

/**
 * 流读iceberg v2表
 */
public class SelectJob2 {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
		config.set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.setParallelism(1);
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		tenv.getConfig().addConfiguration(config);

		String sql = FileUtils.readFile("myuser2.sql");
		tenv.executeSql(sql);
//		Table table = tenv.sqlQuery("SELECT * FROM myuser2 /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/");
		// No Ranking Output Optimization 不要在外层select rownum
		Table table = tenv.sqlQuery("select id, name, audit, update_time from (" +
				" select *," +
				" row_number() over (partition by id order by update_time desc) as rownum" +
				" from myuser2 /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/)" +
				" where rownum = 1");

		ExternalSchemaTranslator.OutputResult schemaTranslationResult =
				ExternalSchemaTranslator.fromInternal(table.getResolvedSchema(), null);
		tenv.toChangelogStream(table, schemaTranslationResult.getSchema(), ChangelogMode.upsert())
				.executeAndCollect()
				.forEachRemaining(System.out::println);

	}

}
