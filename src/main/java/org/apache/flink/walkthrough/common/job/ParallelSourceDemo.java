package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.walkthrough.common.source.ParallelSource1;

import java.util.concurrent.TimeUnit;

/**
 * @author zhang lianhui
 * @date 2020/11/12 6:56 下午
 */
public class ParallelSourceDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStreamSource<Long> source = env.addSource(new ParallelSource1()).setParallelism(4);
		final DataStream<Long> stream = source.map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				System.out.println("received value： " + value);
				return value;
			}
		});

		final DataStream<Long> sumStream = stream
				.timeWindowAll(Time.of(2, TimeUnit.SECONDS))
				.sum(0);

		sumStream.printToErr("sum").setParallelism(1);
		env.execute("parallel source demo");
	}
}
