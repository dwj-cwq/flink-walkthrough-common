package org.apache.flink.walkthrough.common.job;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.utils.Lists;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.entity.Rule;
import org.apache.flink.walkthrough.common.functionImp.MapToRecord;
import org.apache.flink.walkthrough.common.functionImp.MedianFinder;
import org.apache.flink.walkthrough.common.source.MonitorSource;
import org.apache.flink.walkthrough.common.util.TimeUtil;
import org.apache.flink.walkthrough.common.util.Util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhang lianhui
 * @date 2020/11/3 4:45 下午
 */
@Slf4j
public class EngineDemo3 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置默认并行度
		env.setParallelism(1);
		// 使用 event time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// check point every 5 seconds
		env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);

		// 每5000毫秒生成一个Watermark
		env.getConfig().setAutoWatermarkInterval(5000L);

		// kafka topic
		final String topic = "huidemo3";

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

		// monitor source
		final DataStream<Rule> ruleStream = env.addSource(new MonitorSource());

		// 生成水印

		// 流合并
		final ConnectedStreams<Rule, Record> connect = ruleStream.connect(recordsStream);

		// key by monitor id
		final DataStream<Tuple2<String, Double>> process = connect
				.keyBy(Rule::getMonitorId, Record::getMonitorId)
				.process(new KeyedCoProcessFunction<String, Rule, Record, Tuple2<String, Double>>() {
					MapState<String, Record> cachedRecord;

					@Override
					public void processElement1(
							Rule rule,
							Context ctx,
							Collector<Tuple2<String, Double>> out) throws Exception {
						log.info("控制流: " + rule + " 进入");
						if (!cachedRecord.isEmpty()) {
							final Iterable<Map.Entry<String, Record>> entries = cachedRecord.entries();
							final long cutOff = rule.getCutOffTimestamp();
							MedianFinder medianFinder = new MedianFinder();
							for (Iterator<Map.Entry<String, Record>> entryIterator = entries.iterator(); entryIterator
									.hasNext(); ) {
								final Map.Entry<String, Record> curr = entryIterator.next();
								final Record value = curr.getValue();
								if (value.getTimestamp() < cutOff) {
									log.info("remove: " + value + ", cut off timestamp: " + TimeUtil
											.epochMilliFormat(cutOff));
									entryIterator.remove();
								} else {
									medianFinder.addNum(value.getValue());
								}
							}
							// 输出中位数
							if (medianFinder.size() > 0) {
								final double median = medianFinder.getMedian();
								List<Double> records = Lists.newArrayList();
								cachedRecord.values().forEach(r -> records.add(r.getValue()));
								records.sort(Double::compareTo);
								log.info(String.format(
										"current key: %s, media size: %d, media value: %s, records: %s",
										rule.getMonitorId(),
										medianFinder.size(),
										median,
										records));
								out.collect(Tuple2.of(rule.getMonitorId(), median));
							}
						}
					}

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);
						MapStateDescriptor<String, Record> mapStateDescriptor = new MapStateDescriptor<String, Record>(
								"cached_record",
								String.class,
								Record.class);
						StateTtlConfig ttl = StateTtlConfig
								// 设置有效期为 30 分钟
								.newBuilder(Time.minutes(30))
								// 设置有效期更新规则，这里设置为当创建和写入时，都重置其有效期
								.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
								.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
								.build();
						mapStateDescriptor.enableTimeToLive(ttl);
						cachedRecord = getRuntimeContext().getMapState(mapStateDescriptor);
					}

					@Override
					public void processElement2(
							Record value,
							Context ctx,
							Collector<Tuple2<String, Double>> out) throws Exception {
						if (value.getValue() != null && value.getTimestamp() != null) {
							cachedRecord.put(Util.getUuid(), value);
						}

					}
				});


		// sink operator
		process.printToErr("sink");

		// 执行任务
		env.execute("engine job demo");
	}
}
