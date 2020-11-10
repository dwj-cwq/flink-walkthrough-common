package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

/**
 * @author zhang lianhui
 * @date 2020/11/10 3:06 下午
 */
@Slf4j
public abstract class ShutdownAbleThread extends Thread {
	private CountDownLatch shutdownInitiated = new CountDownLatch(1);
	private CountDownLatch shutdownCompleted = new CountDownLatch(1);
	private String name;
	private volatile boolean isStarted = false;
	private boolean isInterruptible;

	public ShutdownAbleThread(String threadName) {
		this(threadName, true);
	}

	public ShutdownAbleThread(String threadName, boolean isInterruptible) {
		super(String.format("[ %s ]", threadName));
		this.name = String.format("[ %s ]", threadName);
		this.isInterruptible = isInterruptible;
	}

	/**
	 * 销毁线程
	 */
	public void shutdown() {
		initShutdown();
		awaitShutdown();
	}

	/**
	 * @return shutdown 是否开启
	 */
	public boolean isShutdownInit() {
		return shutdownInitiated.getCount() == 0;
	}

	/**
	 * @return shutdown 是否完成
	 */
	public boolean isShutdownComplete() {
		return shutdownCompleted.getCount() == 0;
	}


	/**
	 * @return 线程是否在运行
	 */
	public boolean isRunning() {
		return !isShutdownInit();
	}

	public synchronized boolean initShutdown() {
		if (isRunning()) {
			log.info("{} shutting down", name);
			shutdownInitiated.countDown();
			if (isInterruptible) {
				super.interrupt();
			}
			return true;
		}
		return false;
	}

	public void awaitShutdown() {
		if (!isShutdownInit()) {
			throw new IllegalStateException(" initShutdown() was not called before awaitShutdown()");
		}
		if (isStarted) {
			try {
				shutdownCompleted.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			log.info("{} shutdown completed ", name);
		}
	}

	/**
	 * This method is repeatedly invoked until the thread shuts down or this method throws an exception
	 */
	public abstract void doWork();

	@Override
	public void run() {
		isStarted = true;
		log.info("{} Starting", name);
		try {
			while (isRunning()) {
				doWork();
			}
		} catch (Exception ex) {
			shutdownInitiated.countDown();
			shutdownCompleted.countDown();
			log.error("{} error due to {}", name, ex.getMessage(), ex);
		} finally {
			shutdownCompleted.countDown();
		}
		log.info("{} Stopped", name);
	}
}
