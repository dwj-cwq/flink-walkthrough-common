package org.apache.flink.walkthrough.common.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.SensorEvent;

/**
 * @author zhang lianhui
 * @date 2020/10/28 11:50 上午
 */
public class SinkToEsJob {
	public static void main(String[] args) {
		// step1 : set env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// step 2: set source
		final DataStream<String> source = env.readTextFile(
				"/Users/shenlong/Documents/flink-master/flink-walkthroughs/flink-walkthrough-common/src/main/resources/sensor.txt");

		// step 3: transform
		final DataStream<SensorEvent> dataStream = source.map(line -> {
			final String[] items = line.split(",");
			String id = items[0];
			Long timestamp = Long.parseLong(items[1].trim());
			Double temperature = Double.parseDouble(items[2]);
			return new SensorEvent(id, timestamp, temperature);
		});

		// step 4: sink



	}
}
