package org.apache.flink.walkthrough.common.windowAssingerImp;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.windowImp.EngineWindow;

import java.util.Collection;

/**
 * @author zhang lianhui
 * @date 2020/10/29 4:18 下午
 */
// TODO: 2020/10/29 to be implement
public class EngineWindowAssigner extends WindowAssigner<Record, EngineWindow> {
	@Override
	public Collection<EngineWindow> assignWindows(
			Record element,
			long timestamp,
			WindowAssignerContext context) {
		return null;
	}

	@Override
	public Trigger<Record, EngineWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return null;
	}

	@Override
	public TypeSerializer<EngineWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return null;
	}

	@Override
	public boolean isEventTime() {
		return false;
	}
}
