package org.apache.flink.walkthrough.common.job;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.entity.RecordType;
import org.apache.flink.walkthrough.common.functionImp.MapToRecord;
import org.apache.flink.walkthrough.common.functionImp.MedianFinder;
import org.apache.flink.walkthrough.common.source.MonitorSource;
import org.apache.flink.walkthrough.common.util.TimeUtil;
import org.apache.flink.walkthrough.common.watermarkImp.EngineWatermarkImp;

import java.util.Properties;

/**
 * @author zhang lianhui
 * @date 2020/10/30 2:48 下午
 */
@Slf4j
public class EngineDemo4 {
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
		final DataStream<Record> monitorStream = env.addSource(new MonitorSource());

		// 生成水印
		final DataStream<Record> monitorStreamWithWatermarks = monitorStream.assignTimestampsAndWatermarks(
				new EngineWatermarkImp().withTimestampAssigner((r, t) -> r.getTimestamp()));

		// 流合并
		final DataStream<Record> unionStream = recordsStream.union(monitorStreamWithWatermarks);

		// key by monitor id
		final DataStream<Double> process = unionStream
				.keyBy(Record::getMonitorId)
				.window(GlobalWindows.create())
				.trigger(new Trigger<Record, GlobalWindow>() {
					// TODO: 2020/11/2 后续会根据需求进行调整
					@Override
					public TriggerResult onElement(
							Record element,
							long timestamp,
							GlobalWindow window,
							TriggerContext ctx) throws Exception {
						if (element.getRecordType() == RecordType.MONITOR) {
							System.out.println("当前 Monitor：" + element);
							return TriggerResult.FIRE;
						}
						return TriggerResult.CONTINUE;
					}

					@Override
					public TriggerResult onProcessingTime(
							long time,
							GlobalWindow window,
							TriggerContext ctx) throws Exception {
						return TriggerResult.CONTINUE;
					}

					// TODO: 2020/11/2 设置窗口触发条件
					@Override
					public TriggerResult onEventTime(
							long time,
							GlobalWindow window,
							TriggerContext ctx) throws Exception {
						return TriggerResult.CONTINUE;
					}

					// TODO: 2020/11/2 clear window etc
					@Override
					public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

					}
				})
				.evictor(new Evictor<Record, GlobalWindow>() {
					@Override
					public void evictBefore(
							Iterable<TimestampedValue<Record>> elements,
							int size,
							GlobalWindow window,
							EvictorContext evictorContext) {
						
					}

					@Override
					public void evictAfter(
							Iterable<TimestampedValue<Record>> elements,
							int size,
							GlobalWindow window,
							EvictorContext evictorContext) {

					}
				})
				.process(new ProcessWindowFunction<Record, Double, String, GlobalWindow>() {
					@Override
					public void process(
							String key,
							Context context,
							Iterable<Record> elements,
							Collector<Double> out) throws Exception {
						log.info("current window max ts:" + TimeUtil.epochMilliFormat(context
								.window()
								.maxTimestamp()));
						log.info("current watermark: "
								+ TimeUtil.epochMilliFormat(context.currentWatermark())
								+ ", processing time: "
								+ TimeUtil.epochMilliFormat(context.currentProcessingTime()));
						// 执行中位数计算
						final MedianFinder medianFinder = new MedianFinder();
						for (Record record : elements) {
							if (record.getValue() != null) {
								medianFinder.addNum(record.getValue());
							}
						}
						log.info(
								"key:" + key + " ,heap size: " + medianFinder.size() + ", median: "
										+ medianFinder.getMedian());
						// 输出中位数
						out.collect(medianFinder.getMedian());
					}
				});

		// sink operator
		process.printToErr("sink:");

		// 执行任务
		env.execute("engine job demo");
	}
}
