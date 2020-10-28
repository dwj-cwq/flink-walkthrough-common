package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhang lianhui
 * @date 2020/10/27 10:59 下午
 */
public class ReduceDemoJob {
	public static void main(String[] args) throws Exception {
		List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
		data.add(new Tuple3<>(0,1,0));
		data.add(new Tuple3<>(0,1,1));
		data.add(new Tuple3<>(0,2,2));
		data.add(new Tuple3<>(0,1,3));
		data.add(new Tuple3<>(1,2,5));
		data.add(new Tuple3<>(1,2,9));
		data.add(new Tuple3<>(1,2,11));
		data.add(new Tuple3<>(1,2,13));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple3<Integer, Integer, Integer>> stream = env.fromCollection(data);
		final DataStream<Tuple3<Integer, Integer, Integer>> reduce = stream
				.keyBy(t -> t.f0)
				.reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
					@Override
					public Tuple3<Integer, Integer, Integer> reduce(
							Tuple3<Integer, Integer, Integer> t1,
							Tuple3<Integer, Integer, Integer> t2) throws Exception {
						return new Tuple3<>(0, 0, t1.f2 + t2.f2);
					}
				});
		reduce.print();
		env.execute("Reduce demo job");
	}
}
