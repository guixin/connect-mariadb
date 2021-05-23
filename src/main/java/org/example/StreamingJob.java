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

package org.example;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJob.class);

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		JDBCAppendTableSink jdbcSink = JDBCAppendTableSink.builder()
				.setDBUrl("jdbc:mariadb://192.168.31.226:3306/flink_connect_mariadb")
				.setDrivername("org.mariadb.jdbc.Driver")
				.setPassword("")
				.setUsername("")
				.setQuery("insert into t_temp (rand_val) value (?)")
				.setParameterTypes(BasicTypeInfo.STRING_TYPE_INFO)
				.setBatchSize(1)
				.build();

		Table randTable = tableEnv.fromDataStream(env.addSource(new RandValGenerator()));
		tableEnv.registerTableSink("jdbcSink", new String[]{"rand_val"}, new TypeInformation[]{Types.STRING}, jdbcSink);
		tableEnv.registerTable("randTable", randTable);
		randTable.insertInto("jdbcSink");

		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class RandValGenerator implements SourceFunction<String> {
		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			while (true) {
				String randVal = UUID.randomUUID().toString();
				sourceContext.collect(randVal);
				System.out.println("rand value: " + randVal);
				Thread.sleep(1000);
			}
		}

		@Override
		public void cancel() {

		}
	}
}
