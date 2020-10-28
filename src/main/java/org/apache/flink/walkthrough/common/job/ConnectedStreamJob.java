package org.apache.flink.walkthrough.common.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang lianhui
 * @date 2020/10/21 8:15 下午
 */
public class ConnectedStreamJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> control = env.fromElements("DROP", "IGNORE", "1", "2", "3").keyBy(x -> x);
		DataStream<String> streamOfWords = env
				.fromElements("Apache", "DROP", "Flink", "IGNORE", "1", "2", "4")
				.keyBy(x -> x);

		control
				.connect(streamOfWords)
				.flatMap(new ControlFunction())
				.print("测试：");

		env.execute();
	}
}
