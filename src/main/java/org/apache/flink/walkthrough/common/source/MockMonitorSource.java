package org.apache.flink.walkthrough.common.source;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.walkthrough.common.entity.Record;

/**
 * @author zhang lianhui
 * @date 2020/10/29 4:38 下午
 */
public class MockMonitorSource extends FromIteratorFunction<Record> {

	private static final long serialVersionUID = -6569393217427722785L;

	public MockMonitorSource(String id, long initTimestamp, Time time) {
		super(new MockMonitorIterator(id, initTimestamp, time));
	}
}
