package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.functionImp.MapToRecord;
import org.apache.flink.walkthrough.common.watermarkImp.EngineWatermarkImp;
import org.apache.flink.walkthrough.common.windowAssingerImp.EngineWindowAssigner;

import java.util.Properties;

/**
 * @author zhang lianhui
 * @date 2020/10/29 3:22 下午
 */
public class EngineJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置默认并行度
		env.setParallelism(1);
		// 使用 event time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// check point every 5seconds
		env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);

		// kafka topic
		final String topic = "huidemo";

		// kafka properties
		Properties properties = new Properties();
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test-group");
		properties.setProperty(
				"key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty(
				"value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		//偏移量自动重置
		properties.setProperty("auto.offset.reset", "latest");

		// 读取 kafka 的消息
		final DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer011<String>(
				topic,
				new SimpleStringSchema(),
				properties));

		// kafka message demo
		// {
		//  "monitor_id":"xxx",  // String type
		//  "timestamp": 1603956549000  // long type, ms
		//  "value":10.01  // double type
		//}

		// kafka message 反序列化
		final DataStream<Record> recordsStream = kafkaSource.map(new MapToRecord());



		// 生成水印
		final DataStream<Record> recordsWithWatermark = recordsStream.assignTimestampsAndWatermarks(
				new EngineWatermarkImp().withTimestampAssigner((r, t) -> r.getTimestamp()));

		// key by monitor id
//
//		recordsWithWatermark.keyBy(Record::getMonitorId).window(new EngineWindowAssigner()).aggregate(
//				new AggregateFunction<Record, Object, Object>() {
//					@Override
//					public Object createAccumulator() {
//							return null;
//					}
//
//					@Override
//					public Object add(Record value, Object accumulator) {
//						return null;
//					}
//
//					@Override
//					public Object getResult(Object accumulator) {
//						return null;
//					}
//
//					@Override
//					public Object merge(Object a, Object b) {
//						return null;
//					}
//				})

		// aggregate, compute the middle number

		// 执行任务
		env.execute("engine job demo");

	}
}
