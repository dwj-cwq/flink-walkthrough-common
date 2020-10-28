package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhang lianhui
 * @date 2020/10/27 8:12 下午
 */
public class KeyByAndMaxJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
		data.add(new Tuple3<>(0,1,0));
		data.add(new Tuple3<>(0,1,1));
		data.add(new Tuple3<>(0,2,2));
		data.add(new Tuple3<>(0,1,3));
		data.add(new Tuple3<>(1,2,5));
		data.add(new Tuple3<>(1,2,9));
		data.add(new Tuple3<>(1,2,11));
		data.add(new Tuple3<>(1,2,13));

		DataStream<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(data);
		final DataStream<Tuple3<Integer, Integer, Integer>> stream = source
				.keyBy(v -> v.f0)
				.max(2);

		stream.print();
		env.execute("min and max job");


	}
}
