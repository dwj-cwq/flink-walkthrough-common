package org.apache.flink.walkthrough.common.job;

import com.google.common.collect.Lists;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Monitor;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.entity.RecordImp;
import org.apache.flink.walkthrough.common.entity.SimpleMonitor;

import java.util.List;

/**
 * @author zhang lianhui
 * @date 2020/10/29 5:06 下午
 */
public class EngineJobTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<Monitor> monitors = Lists.newArrayList(
				new SimpleMonitor("monitor_a", "0 0/1 * * * ? *", 60 * 1000L),
				new SimpleMonitor("monitor_b", "0/10 * * * * ? ", 20 * 1000L));
		final DataStreamSource<Monitor> monitorSource = env.fromCollection(monitors);
		final DataStream<Record> processStream = monitorSource
				.keyBy(Monitor::getMonitorId)
				.countWindow(1)
				.process(new ProcessWindowFunction<Monitor, Record, String, GlobalWindow>() {
					@Override
					public void process(
							String key,
							Context context,
							Iterable<Monitor> elements,
							Collector<Record> out) throws Exception {
						for (Monitor element : elements) {
							final String cronExpression = element.getCronExpression();
							for (int i = 0; i < 10; i++) {
								out.collect(new RecordImp(
										element.getMonitorId(),
										System.currentTimeMillis(),
										10.0));
							}
						}
					}
				});

		processStream.printToErr();
		env.execute("monitor_source demo");
	}
}
