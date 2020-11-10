package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.walkthrough.common.util.TimeUtil;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTaskList.TimerTaskEntry;

/**
 * 1）采用时间轮做任务的添加、删除 （O(1) 时间复杂度）
 * a. TimerTaskList 是一个双向循环链表，内部有一个 虚拟节点 root 节点
 * 1.1) root 节点不包含 TimerTask
 * 1.2) root.next 为链表的 head 节点
 * 1.3）root.prev 为链表的 tail 节点
 * 1.4）AtomicInteger taskCounter 任务的计数器
 * 1.5）实现 Delayed 接口，用于将 TimerTaskList 元素插入 DelayQueue， 用 DelayQueue 做时间的推进
 * b. TimerTaskEntry 是双向链表的单个节点，其内部的主要元素有
 * 1.1）TimerTaskList list 所在的链表
 * 1.2）TimerTaskEntry prev 指向前一个节点
 * 1.3）TimerTaskEntry next 指向下一个节点
 * 1.4）TimerTask timerTask 包装的 Runnable 任务
 * 1.5）Long expirationMs 过期时间，单位 Ms
 * 2）采用 DelayQueue 做时间的推进：
 * a. 获取队列 head 的元素只需要 O(1) 的时间复杂度
 * b. 以少量空间换时间， 延迟队列的内部实现采用了优先级队列（最小堆）
 *
 * @author kafka
 * @date 2020/11/9 3:58 下午
 */
@Slf4j
public class SystemTimer extends Timer {
	private static final long DEFAULT_TICK_MS = 1;
	private static final int DEFAULT_WHEEL_SIZE = 20;
	private final String executorName;
	private final long tickMs;
	private final int wheelSize;
	private final long startMs;
	private final AtomicInteger taskCounter;
	private final DelayQueue<TimerTaskList> delayQueue;
	private final TimingWheel timingWheel;

	/**
	 * thread settings
	 */
	private ThreadFactory threadFactory;

	/**
	 * time out timer
	 */
	private ExecutorService executorService;

	/**
	 * locks used to protect data structures while ticking
	 */
	private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	private Lock readLock = readWriteLock.readLock();
	private Lock writeLock = readWriteLock.writeLock();

	public SystemTimer() {
		this("default_system_timer");
	}

	public SystemTimer(String executorName) {
		this(executorName, DEFAULT_TICK_MS, DEFAULT_WHEEL_SIZE, TimeUtil.hiResClockMs());
	}

	public SystemTimer(
			String executorName,
			long tickMs,
			int wheelSize,
			long startMs) {
		this.executorName = executorName;
		this.tickMs = tickMs;
		this.wheelSize = wheelSize;
		this.startMs = startMs;
		this.taskCounter = new AtomicInteger(0);
		this.delayQueue = new DelayQueue<>();
		this.timingWheel = new TimingWheel(
				tickMs,
				wheelSize,
				startMs,
				this.taskCounter,
				this.delayQueue
		);
		this.threadFactory = new ThreadFactoryBuilder()
				.setNameFormat("timing wheel thread-" + executorName)
				.build();
		this.executorService = Executors.newFixedThreadPool(1, this.threadFactory);
	}

	public String getExecutorName() {
		return executorName;
	}

	public long getTickMs() {
		return tickMs;
	}

	public int getWheelSize() {
		return wheelSize;
	}

	public long getStartMs() {
		return startMs;
	}

	public AtomicInteger getTaskCounter() {
		return taskCounter;
	}

	public TimingWheel getTimingWheel() {
		return timingWheel;
	}

	@Override
	public void add(TimerTask timerTask) {
		readLock.lock();
		try {
			addTimerTaskEntry(new TimerTaskEntry(
					timerTask,
					TimeUtil.hiResClockMs() + timerTask.getDelayMs()));
		} catch (Exception ex) {
			log.error("add timer task error {}", ex.getMessage(), ex);
		} finally {
			readLock.unlock();
		}
	}

	private void addTimerTaskEntry(TimerTaskEntry taskEntry) {
		if (taskEntry != null) {
			if (!timingWheel.add(taskEntry)) {
				// already expired or cancelled
				if (!taskEntry.cancelled()) {
					executorService.submit(taskEntry.getTimerTask());
				}
			}
		}
	}

	private void reInsert(TimerTaskEntry taskEntry) {
		addTimerTaskEntry(taskEntry);
	}

	/**
	 * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
	 * waits up to timeoutMs before giving up.
	 */
	@Override
	public boolean advanceClock(long timeoutMs) {
		TimerTaskList bucket = null;
		try {
			bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ex) {
			log.error("advance task error {}", ex.getMessage(), ex);
		}

		if (bucket != null) {
			writeLock.lock();
			try {
				while (bucket != null) {
					// log.info("timing wheel advance clock: {}", bucket.getExpiration());
					timingWheel.advanceClock(bucket.getExpiration());
					bucket.flush(this::reInsert);
					bucket = delayQueue.poll();
				}
			} finally {
				writeLock.unlock();
			}
			return true;
		}
		return false;
	}

	@Override
	public int getSize() {
		return taskCounter.get();
	}

	@Override
	public void shutdown() {
		this.executorService.shutdown();
	}
}
