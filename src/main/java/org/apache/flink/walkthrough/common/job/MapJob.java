package org.apache.flink.walkthrough.common.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @author zhang lianhui
 * @date 2020/10/27 7:23 下午
 */
public class MapJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStream<Transaction> demo = env
				.addSource(new TransactionSource())
				.setParallelism(1)
				.name("map_demo");

		final DataStream<Double> map = demo
				.keyBy(v -> v.getAccountId())
				.map(v -> v.getAmount());
		map.print().setParallelism(1);
		env.execute("map_demo");
	}
}
