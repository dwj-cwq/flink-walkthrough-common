package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.SensorEvent;

/**
 * @author zhang lianhui
 * @date 2020/10/28 10:48 上午
 */
public class SensorReadingJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		final DataStream<String> source = env.readTextFile(
				"/Users/shenlong/Documents/flink-master/flink-walkthroughs/flink-walkthrough-common/src/main/resources/sensor.txt");

		final DataStream<SensorEvent> dataStream = source.map(line -> {
			final String[] items = line.split(",");
			String id = items[0];
			Long timestamp = Long.parseLong(items[1].trim());
			Double temperature = Double.parseDouble(items[2]);
			return new SensorEvent(id, timestamp, temperature);
		});

		final KeyedStream<SensorEvent, String> aggStream = dataStream.keyBy(SensorEvent::getId);
		final SingleOutputStreamOperator<SensorEvent> stream1 = aggStream.sum("temperature");

		//  输出当前传感器最新的温度要加10，时间戳是上一次数据的时间加1
		aggStream.reduce(new ReduceFunction<SensorEvent>() {
			@Override
			public SensorEvent reduce(SensorEvent value1, SensorEvent value2) throws Exception {
				return new SensorEvent(value1.getId(), value1.getTimestamp() + 1, value1.getTemperature() +10);
			}
		});

		env.execute("sensor reading job");

	}
}
