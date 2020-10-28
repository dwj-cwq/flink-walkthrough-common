package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang lianhui
 * @date 2020/10/21 8:24 下午
 */
public class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
	private transient ValueState<Boolean> blocked;

	@Override
	public void open(Configuration parameters) throws Exception {
		blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));
	}

	@Override
	public void flatMap1(String control_value, Collector<String> out) throws Exception {
		blocked.update(Boolean.TRUE);

	}

	@Override
	public void flatMap2(String data_value, Collector<String> out) throws Exception {
		if (blocked.value() == null) {
			out.collect(data_value);
		}
	}
}
