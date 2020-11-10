package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTaskList.TimerTaskEntry;

/**
 * Hierarchical Timing Wheels
 * https://www.confluent.io/blog/apache-kafka-purgatory-hierarchical-timing-wheels/
 * @author kafka
 * @date 2020/11/9 1:00 下午
 */
@Slf4j
public class TimingWheel {
	private final long tickMs;
	private final int wheelSize;
	private final long startMs;
	private final AtomicInteger taskCounter;

	private final DelayQueue<TimerTaskList> queue;

	private final long interval;
	private final List<TimerTaskList> buckets;
	private long currentTime;
	/**
	 * overflowWheel can potentially be updated and read by two concurrent threads through add().
	 * Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
	 */
	private volatile TimingWheel overflowWheel;


	public TimingWheel(
			long tickMs,
			int wheelSize,
			long startMs,
			AtomicInteger taskCounter,
			DelayQueue<TimerTaskList> queue) {
		this.tickMs = tickMs;
		this.wheelSize = wheelSize;
		this.startMs = startMs;
		this.taskCounter = taskCounter;
		this.queue = queue;
		this.interval = tickMs * wheelSize;
		this.buckets = new ArrayList<>(wheelSize);
		for (int i = 0; i < wheelSize; i++) {
			buckets.add(new TimerTaskList(taskCounter));
		}
		// rounding down to multiple of tickMs;
		this.currentTime = startMs - (startMs % tickMs);
	}

	private synchronized void addOverflowWheel() {
		if (overflowWheel == null) {
			overflowWheel = new TimingWheel(interval, wheelSize, currentTime, taskCounter, queue);
			//	log.info("create over flow wheel: " + overflowWheel);
		}
	}

	public boolean add(TimerTaskEntry taskEntry) {
		long expirationMs = taskEntry.getExpirationMs();
		if (taskEntry.cancelled()) {
			// cancelled
			return false;
		}

		if (expirationMs < currentTime + tickMs) {
			// already expired
			return false;
		}

		if (expirationMs < currentTime + interval) {
			// put in its own buckets
			long virtualId = expirationMs / tickMs;
			final TimerTaskList bucket = buckets.get((int) (virtualId % wheelSize));
			bucket.add(taskEntry);

			// set the bucket expiration time
			if (bucket.setExpiration(virtualId * tickMs)) {
				// The bucket needs to be enqueued because it was an expired bucket
				// We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
				// and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
				// will pass in the same value and hence return false, thus the bucket with the same expiration will not
				// be enqueued multiple times.
				queue.offer(bucket);
			}

			return true;
		}

		// out of the interval, put it into the parent timer
		if (overflowWheel == null) {
			addOverflowWheel();
		}
		return overflowWheel.add(taskEntry);
	}

	@Override
	public String toString() {
		return "TimingWheel{" +
				"tickMs=" + tickMs +
				", wheelSize=" + wheelSize +
				", startMs=" + startMs +
				", taskCounter=" + taskCounter +
				", interval=" + interval +
				", currentTime=" + currentTime +
				'}';
	}

	/**
	 * 驱动时间轮前进
	 *
	 * @param timeMs try to advance the clock
	 */
	public void advanceClock(long timeMs) {
		if (timeMs >= currentTime + tickMs) {
			currentTime = timeMs - (timeMs % tickMs);
			//	log.info("advance the clock: {}, tickMs: {}", currentTime, tickMs);
			// try to advance the clock of the overflow wheel if present
			if (overflowWheel != null) {
				overflowWheel.advanceClock(currentTime);
			}
		}
	}
}
