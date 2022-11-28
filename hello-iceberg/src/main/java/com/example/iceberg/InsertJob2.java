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
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;


public class InsertJob2 {

	public static void main(String[] args) throws Exception {
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		String sql = FileUtils.readFile("myuser2.sql");
		tenv.executeSql(sql);

		try {
			Configuration conf = new Configuration();
			String warehousePath = "hdfs://localhost:9000/user/iceberg";
			HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
			Table table = catalog.loadTable(TableIdentifier.of("default_database", "myuser2"));
			Calendar calendar = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00.000");
			String dateStr = sdf.format(calendar.getTime());
			System.out.println(dateStr);

			Actions.forTable(table).rewriteDataFiles()
					.filter(Expressions.lessThan("update_time", dateStr))
					//.option("target-file-size-bytes", Long.toString(512 * 1024 * 1024))
					.execute();
		} catch (NoSuchTableException e) {
			e.printStackTrace();
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		while(true) {
			int no = new Random().nextInt(10);
			String name = UUID.randomUUID().toString().substring(0, 4);
			boolean audit = new Random().nextBoolean();
			String insertSql = String.format("insert into myuser2 values(%d, '%s', %s, LOCALTIMESTAMP )",
					no, name, audit);
//			Calendar cal = Calendar.getInstance();
//			cal.add(Calendar.DAY_OF_YEAR, -1);
//			System.out.println(sdf.format(cal.getTime()));
//			String insertSql = String.format("insert into myuser2 values(%d, '%s', %s, TIMESTAMP '%s')",
//					no, name, audit, sdf.format(cal.getTime()));
			System.out.println(insertSql);
			TableResult result = tenv.executeSql(insertSql);
			result.await();
			Thread.sleep(300);
		}

	}

}
