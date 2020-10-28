package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang lianhui
 * @date 2020/10/24 11:15 上午
 */
public class CountWindowJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStreamSource<Tuple2<Long, Long>> stream = env.fromElements(
				Tuple2.of(1L, 3L),
				Tuple2.of(1L, 5L),
				Tuple2.of(1L, 7L),
				Tuple2.of(1L, 4L),
				Tuple2.of(1L, 2L));

		stream.keyBy(v -> v.f0)
				.flatMap(new CountWindowAverage())
				.print();
		env.execute("count window job");
	}

}
