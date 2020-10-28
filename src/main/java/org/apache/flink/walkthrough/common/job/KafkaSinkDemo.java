package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.walkthrough.common.entity.SensorEvent;

import java.util.Properties;

/**
 * @author zhang lianhui
 * @date 2020/10/28 11:22 上午
 */
public class KafkaSinkDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		final String topic = "huidemo";

		Properties properties = new Properties();
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test-group");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//偏移量自动重置
		properties.setProperty("auto.offset.reset", "latest");

		final DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>(
				topic,
				new SimpleStringSchema(),
				properties));


		final DataStream<String> dataStream = source.map(line -> {
			final String[] items = line.split(",");
			String id = items[0];
			Long timestamp = Long.parseLong(items[1].trim());
			Double temperature = Double.parseDouble(items[2]);
			return new SensorEvent(id, timestamp, temperature).toString();
		});

		dataStream.addSink(new FlinkKafkaProducer011<String>(topic, "sinkTest", new SimpleStringSchema()));
		dataStream.print();
		env.execute("kafka sink test");
	}
}
