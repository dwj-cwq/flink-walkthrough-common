package org.apache.flink.walkthrough.common.job;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.source.MonitorSource;

/**
 * @author zhang lianhui
 * @date 2020/10/30 11:05 上午
 */
public class EngineSourceDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		final DataStream<Record> streamSource = env.addSource(new MonitorSource());

		streamSource.printToErr();
		env.execute("monitor source");
	}
}
