package org.apache.flink.walkthrough.common.job;

import com.google.common.base.Objects;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author zhang lianhui
 * @date 2019-08-19 20:28
 */
public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {
		final int port;
		try {
			final ParameterTool parameterTool = ParameterTool.fromArgs(args);
			port = parameterTool.getInt("port");
		} catch (Exception ex) {
			System.err.println("Not port specified. Please run 'SocketWindowWordCount --port <port>'");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		DataStream<String> text = env.socketTextStream("localhost", port, "\n");

		DataStream<WordWithCount> windowCounts = text
				.flatMap(new Splitter())
				.keyBy("word")
				.timeWindow(Time.seconds(5), Time.seconds(1))
				.reduce(new Reducer());

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		env.execute("Socket Window WordCount");
	}

	public static class Splitter implements FlatMapFunction<String, WordWithCount> {

		@Override
		public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
			for (String word : line.split("\\s+")) {
				collector.collect(new WordWithCount(word, 1));
			}
		}
	}

	public static class Reducer implements ReduceFunction<WordWithCount> {
		@Override
		public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
			return new WordWithCount(w1.getWord(), w1.getCount() + w2.getCount());
		}
	}

	// Data type for words with count

	public static class WordWithCount {

		private String word;

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		private long count;

		@Override
		public String toString() {
			return "WordWithCount{" +
					"word='" + word + '\'' +
					", count=" + count +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WordWithCount that = (WordWithCount) o;
			return count == that.count &&
					Objects.equal(word, that.word);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(word, count);
		}
	}
}
