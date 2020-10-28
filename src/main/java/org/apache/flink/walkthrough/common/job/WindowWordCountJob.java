package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author zhang lianhui
 * @date 2019-08-20 00:10
 */
public class WindowWordCountJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<String, Integer>> dataStream = env
				.socketTextStream("localhost", 9000, "\n")
				.flatMap(new Splitter())
				.keyBy(0)
				.timeWindow(Time.seconds(5))
				.sum(1);

		dataStream.print();

		env.execute("Window WordCount");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
			System.out.println("line" + line);
			for (String word : line.toLowerCase().split("\\W+ ")) {
				collector.collect(new Tuple2<>(word, 1));
			}
		}
	}
}
