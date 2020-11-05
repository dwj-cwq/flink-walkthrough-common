package org.apache.flink.walkthrough.common.source;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.entity.RecordImp;
import org.apache.flink.walkthrough.common.entity.RecordType;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * @author zhang lianhui
 * @date 2020/10/29 4:39 下午
 */

public class MockMonitorIterator implements Iterator<Record>, Serializable {
	private static final long serialVersionUID = 5220993550633399231L;
	private long timestamp;
	private final String id;
	private final Time interval;
	private final Random random = new Random();

	public MockMonitorIterator(String id, long initTimestamp, Time interval) {
		this.timestamp = initTimestamp;
		this.id = id;
		this.interval = interval;
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public Record next() {
		final RecordImp recordImp = new RecordImp(id, timestamp, random.nextDouble()*10);
		timestamp += interval.toMilliseconds();
		try {
			Thread.sleep(interval.toMilliseconds());
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		return recordImp;
	}
}
