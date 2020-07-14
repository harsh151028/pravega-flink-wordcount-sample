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

package com.harsh;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import com.harsh.flink.Utils;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {


		// initialize the parameter utility tool in order to retrieve input parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		PravegaConfig pravegaConfig = PravegaConfig
				.fromParams(params)
				.withDefaultScope(Constants.DEFAULT_SCOPE);

		// create the Pravega input stream (if necessary)
		Stream stream = Utils.createStream(
				pravegaConfig,
				params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));



		// initialize the Flink execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// create the Pravega source to read a stream of text
		FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
				.withPravegaConfig(pravegaConfig)
				.forStream(stream)
				.withDeserializationSchema(PravegaSerialization.deserializationFor(String.class))
				.build();


		// count each word over a 10 second time period
		DataStream<WordCount> dataStream = env.addSource(source).name("Pravega Stream")
				.flatMap(new StreamingJob.Splitter())
				.keyBy("word")
				.timeWindow(Time.seconds(10))
				.sum("count");

		// create an output sink to print to stdout for verification
		dataStream.print();
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
	// split data into word by space
	private static class Splitter implements FlatMapFunction<String, WordCount> {
		@Override
		public void flatMap(String line, Collector<WordCount> out) throws Exception {
			for (String word: line.split(Constants.WORD_SEPARATOR)) {
				out.collect(new WordCount(word, 1));
			}
		}
	}
}
