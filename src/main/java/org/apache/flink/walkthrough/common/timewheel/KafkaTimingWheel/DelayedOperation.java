package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhang lianhui
 * @date 2020/11/10 3:51 下午
 */
public abstract class DelayedOperation extends TimerTask {
	private long delayMs;
	private final Lock lock = new ReentrantLock();
	private final AtomicBoolean completed = new AtomicBoolean(false);

	public DelayedOperation(long delayMs) {
		this.delayMs = delayMs;
	}

	@Override
	public long getDelayMs() {
		return delayMs;
	}

	@Override
	public void setDelayMs(long delayMs) {
		this.delayMs = delayMs;
	}

	/*
	 * Force completing the delayed operation, if not already completed.
	 * This function can be triggered when
	 *
	 * 1. The operation has been verified to be completable inside tryComplete()
	 * 2. The operation has expired and hence needs to be completed right now
	 *
	 * Return true iff the operation is completed by the caller: note that
	 * concurrent threads can try to complete the same operation, but only
	 * the first thread will succeed in completing the operation and return
	 * true, others will still return false
	 */

	public boolean forceComplete() {
		if (completed.compareAndSet(false, true)) {
			// cancel the timeout timer
			cancel();
			onComplete();
			return true;
		}
		return false;
	}

	/**
	 * Check if the delayed operation is already completed
	 */
	public boolean isCompleted() {
		return completed.get();
	}

	/**
	 * Call-back to execute when a delayed operation gets expired and hence forced to complete.
	 */
	public abstract void onExpiration();

	/**
	 * Process for completing an operation; This function needs to be defined
	 * in subclasses and will be called exactly once in forceComplete()
	 */
	public abstract void onComplete();


	/*
	 * run() method defines a task that is executed on timeout
	 */
	@Override
	public void run() {
		if (forceComplete()) {
			onExpiration();
		}
	}

	/**
	 * 后台线程收割过期的延迟任务
	 */
	public static class ExpiredOperationReaper extends ShutdownAbleThread {

		public ExpiredOperationReaper(String threadName) {
			super(threadName, false);
		}

		@Override
		public void doWork() {

		}
	}
}
