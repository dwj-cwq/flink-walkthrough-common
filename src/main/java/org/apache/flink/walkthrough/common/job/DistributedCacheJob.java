package org.apache.flink.walkthrough.common.job;

import org.apache.commons.io.FileUtils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhang lianhui
 * @date 2020/10/28 2:04 下午
 */
public class DistributedCacheJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String filepath = "/Users/shenlong/Documents/flink-master/flink-walkthroughs/flink-walkthrough-common/src/main/resources/cache.txt";
		// 1. 注册一个文件可以是 hdfs 上的文件也可以是本地文件， 然后给注册的文件起一个名字
		env.registerCachedFile(filepath, "cache_file");
		env.setRestartStrategy(RestartStrategies.noRestart());
		System.out.println("time characteristic: " + env.getStreamTimeCharacteristic());


		final DataStream<String> data = env.fromElements(
				"1",
				"2",
				"3",
				"4");

		final DataStream<String> stream = data.map(new RichMapFunction<String, String>() {
			List<String> dataList = new ArrayList<>();

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);

				// 2、getRuntimeContext() 使用该缓存文件，根据注册的名字直接获取文件
				File file = getRuntimeContext().getDistributedCache().getFile("cache_file");
				List<String> lines = FileUtils.readLines(file);
				for (String line : lines) {
					dataList.add(line);
					System.out.println("分布式缓存为：" + line);
				}

			}

			@Override
			public String map(String s) throws Exception {
				//在这里就可以使用dataList
				System.out.println("使用dataList" + dataList + "------" + s);
				return dataList + ":" + s;
			}


		});

		stream.printToErr();

		env.execute("distributed cache test");
	}
}
