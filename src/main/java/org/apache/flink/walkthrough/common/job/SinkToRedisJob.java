package org.apache.flink.walkthrough.common.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.walkthrough.common.entity.SensorEvent;

/**
 * @author zhang lianhui
 * @date 2020/10/28 11:37 上午
 */
public class SinkToRedisJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		final DataStream<String> source = env.readTextFile(
				"/Users/shenlong/Documents/flink-master/flink-walkthroughs/flink-walkthrough-common/src/main/resources/sensor.txt");

		final DataStream<SensorEvent> dataStream = source.map(line -> {
			final String[] items = line.split(",");
			String id = items[0];
			Long timestamp = Long.parseLong(items[1].trim());
			Double temperature = Double.parseDouble(items[2]);
			return new SensorEvent(id, timestamp, temperature);
		});

		final FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
				.setHost("localhost")
				.setPort(6379)
				.build();

		final KeyedStream<SensorEvent, String> aggStream = dataStream.keyBy(SensorEvent::getId);

		dataStream.addSink(new RedisSink<>(conf, new MyRedisMapper()));

		env.execute("redis sink demo");

	}

	public static class MyRedisMapper implements RedisMapper<SensorEvent> {
		// 定义保存数据到 redis 的命令
		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature");
		}

		// 定义 redis  key
		@Override
		public String getKeyFromData(SensorEvent sensorEvent) {
			return sensorEvent.getId();
		}

		// 定义 redis value
		@Override
		public String getValueFromData(SensorEvent sensorEvent) {
			return sensorEvent.getTemperature().toString();
		}
	}
}
