package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTaskList.TimerTaskEntry;

/**
 * Hierarchical Timing Wheels
 * https://www.confluent.io/blog/apache-kafka-purgatory-hierarchical-timing-wheels/
 * https://juejin.im/entry/6844903616671662088
 * https://www.mdeditor.tw/pl/pDvD
 *
 * TimingWheel is not thread safe
 * 时间轮层级越低，时间精度越高；时间轮层级越高，时间精度越高
 * <p>
 * 若第一层 时间轮 tickMs = 1ms， wheelSize = 60，并且有七层时间轮： 则可以表示
 * (60**7)/1000/60/60/24/365 = 88.76年
 *
 * DelayQueue 的引入是为了解决"空推进" 的问题
 *
 * @author kafka
 * @date 2020/11/9 1:00 下午
 */
public class TimingWheel {
	private static final Logger log = LoggerFactory.getLogger(TimingWheel.class);
	/**
	 * 时间槽的单位时间
	 */
	private final long tickMs;

	/**
	 * 时间轮的大小
	 */
	private final int wheelSize;

	/**
	 * 时间轮的启动时间，单调时钟时间，单位 Ms
	 */
	private final long startMs;

	/**
	 * 任务数量，即所有桶的节点数量之和
	 */
	private final AtomicInteger taskCounter;

	/**
	 * timer 与 所有时间轮共享的一个延迟队列
	 */
	private final DelayQueue<TimerTaskList> queue;

	/**
	 * 此层时间轮的跨度
	 */
	private final long interval;

	/**
	 * 此层时间轮的槽/桶
	 */
	private final List<TimerTaskList> buckets;

	/**
	 * 时间轮的时间指针
	 */
	private long currentTime;

	/**
	 * overflowWheel can potentially be updated and read by two concurrent threads through add().
	 * Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
	 * 上一层时间轮、父级时间轮，使用 volatile 限制指令重排序
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

	/**
	 * 创建父级时间轮
	 */
	private synchronized void addOverflowWheel() {
		if (overflowWheel == null) {
			overflowWheel = new TimingWheel(interval, wheelSize, currentTime, taskCounter, queue);
			//	log.info("create over flow wheel: " + overflowWheel);
		}
	}

	/**
	 * 添加任务到时间轮： 若添加成功则返回 true，否则返回 false
	 *
	 * @param taskEntry 单个任务
	 *
	 * @return
	 */
	public boolean add(TimerTaskEntry taskEntry) {
		long expirationMs = taskEntry.getExpirationMs();
		if (taskEntry.cancelled()) {
			// 任务被取消
			return false;
		}

		if (expirationMs < currentTime + tickMs) {
			// 任务已经过期
			return false;
		}

		if (expirationMs < currentTime + interval) {
			// 添加任务到自己的任务槽当中
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

		// 超出本层时间轮间隔，添加任务到父级时间轮中
		if (overflowWheel == null) {
			addOverflowWheel();
		}
		return overflowWheel.add(taskEntry);
	}

	/**
	 * 驱动时间轮前进， 更新时间轮的 currentTime
	 *
	 * @param timeMs try to advance the clock
	 */
	public void advanceClock(long timeMs) {
		if (timeMs >= currentTime + tickMs) {
			/**
			 * 更新时间轮的 currentTime
			 */
			currentTime = timeMs - (timeMs % tickMs);
			//	log.info("advance the clock: {}, tickMs: {}", currentTime, tickMs);
			/**
			 *  如果父级时间轮存在，则更新父级时间轮的 currentTime
			 */
			if (overflowWheel != null) {
				overflowWheel.advanceClock(currentTime);
			}
		}
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
}
