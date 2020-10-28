package org.apache.flink.walkthrough.common.job;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.walkthrough.common.entity.SensorEvent;

/**
 * @author zhang lianhui
 * @date 2020/10/28 4:25 下午
 */
public class SideOutputJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		final DataStream<String> source = env.socketTextStream("localhost", 9999);


		// ==底层也是周期性生成的一个方法 处理乱序数据 延迟1秒种生成水位 同时分配水位和时间戳 括号里传的是等待延迟的时间
		final DataStream<SensorEvent> dataStream = source
				.map(line -> {
					final String[] items = line.split(",");
					String id = items[0];
					Long timestamp = Long.parseLong(items[1].trim());
					Double temperature = Double.parseDouble(items[2]);
					return new SensorEvent(id, timestamp, temperature);
				})
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorEvent>(
						Time.seconds(1)) {
					@Override
					public long extractTimestamp(SensorEvent sensorEvent) {
						return sensorEvent.getTimestamp();
					}
				});

		final SingleOutputStreamOperator<SensorEvent> processStream = dataStream.process(new FreezingAlert());

		// 这里打印的是主流
		processStream.printToErr("process data");
		// 打印侧输出流
		processStream.getSideOutput(new OutputTag<>("Freezing alert")).printToErr();
		processStream.getSideOutput(new OutputTag<>("common data")).printToErr();

		env.execute("side out put job");
	}

	public static class FreezingAlert extends ProcessFunction<SensorEvent, SensorEvent> {
		OutputTag<String> alertOutput = new OutputTag<>("Freezing alert");
		OutputTag<String> commonOutput = new OutputTag<>("common data");

		@Override
		public void processElement(
				SensorEvent sensorEvent,
				Context context,
				Collector<SensorEvent> collector) throws Exception {
			if (sensorEvent.getTemperature() < 32.0) {
				context.output(
						alertOutput,
						sensorEvent.getId() + " 低温报警，此时温度为：" + sensorEvent.getTemperature());
			} else if (sensorEvent.getTemperature() >= 32.0) {
				context.output(
						commonOutput,
						sensorEvent.getId() + " 正常温度，此时温度为：" + sensorEvent.getTemperature());
			} else {
				collector.collect(sensorEvent);
			}
		}
	}
}
