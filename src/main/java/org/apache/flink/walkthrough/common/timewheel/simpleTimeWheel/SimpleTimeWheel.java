package org.apache.flink.walkthrough.common.timewheel.simpleTimeWheel;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.walkthrough.common.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhang lianhui
 * @date 2020/11/5 4:11 下午
 */
@Slf4j
public class SimpleTimeWheel {
	private static final int DEFAULT_RING_SIZE = 64;
	private Object[] ringBuffer;
	private int bufferSize;

	private ExecutorService executorService;

	private AtomicInteger taskSize = new AtomicInteger();
	private AtomicInteger tick = new AtomicInteger();
	private volatile AtomicBoolean start = new AtomicBoolean(false);
	private volatile boolean stop = false;

	private Lock lock = new ReentrantLock();
	private Condition condition = lock.newCondition();

	public SimpleTimeWheel(ExecutorService executorService) {
		this.executorService = executorService;
		this.bufferSize = DEFAULT_RING_SIZE;
		this.ringBuffer = new Object[this.bufferSize];
	}

	public SimpleTimeWheel(ExecutorService executorService, int cap) {
		this.executorService = executorService;
		this.bufferSize = Util.tableSizeFor(cap);
		this.ringBuffer = new Object[this.bufferSize];
	}

	public void addTask(SimpleTask task) {
		final int key = task.getKey();
		try {
			lock.lock();
			List<SimpleTask> tasks = get(key);
			final int cycleNum = comCycleNum(key);
			task.setCycleNum(cycleNum);
			if (tasks != null) {
				tasks.add(task);
			} else {
				tasks = new ArrayList<>();
				tasks.add(task);
				put(key, tasks);
			}
			taskSize.incrementAndGet();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		} finally {
			lock.unlock();
		}
		start();
	}

	private void put(int key, List<SimpleTask> tasks) {
		final int idx = hash(key);
		this.ringBuffer[idx] = tasks;
	}

	private List<SimpleTask> get(int key) {
		int index = hash(key);
		return (List<SimpleTask>) this.ringBuffer[index];
	}

	private int hash(int target) {
		// target % bufferSize
		target = target + tick.get();
		return target & (this.bufferSize - 1);
	}

	private int comCycleNum(int target) {
		// target / bufferSize
		return target >> Integer.bitCount(this.bufferSize - 1);
	}

	private List<SimpleTask> remove(int index) {
		List<SimpleTask> removedTask = new ArrayList<>();
		List<SimpleTask> remainedTask = new ArrayList<>();

		List<SimpleTask> currTasks = (List<SimpleTask>) this.ringBuffer[index];
		if (currTasks == null || currTasks.isEmpty()) {
			return removedTask;
		}

		for (SimpleTask currTask : currTasks) {
			if (currTask.getCycleNum() == 0) {
				removedTask.add(currTask);
				sizeToNotify();
			} else {
				currTask.setCycleNum(currTask.getCycleNum() - 1);
				remainedTask.add(currTask);
			}
		}

		this.ringBuffer[index] = remainedTask;
		return removedTask;
	}

	private void sizeToNotify() {
		try {
			lock.lock();
			int size = taskSize.decrementAndGet();
			if (size <= 0) {
				condition.signal();
			}
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		} finally {
			lock.unlock();
		}
	}

	public void start() {
		if (!start.get()) {

			if (start.compareAndSet(start.get(), true)) {
				log.info("Delay task is starting");
				Thread job = new Thread(new TriggerJob());
				job.setName("consumer RingBuffer thread");
				job.start();
				start.set(true);
			}

		}
	}

	private class TriggerJob implements Runnable {

		@Override
		public void run() {
			int index = 0;
			while (!stop) {
				try {
					List<SimpleTask> tasks = remove(index);
					for (SimpleTask task : tasks) {
						executorService.submit(task);
					}

					if (++index > bufferSize - 1) {
						index = 0;
					}

					//Total tick number of records
					tick.incrementAndGet();
					TimeUnit.SECONDS.sleep(1);

				} catch (Exception e) {
					log.error("Exception", e);
				}

			}

			log.info("Delay task has stopped");
		}
	}


	public static void main(String[] args) {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		SimpleTimeWheel simpleTimeWheel = new SimpleTimeWheel(executorService, 64);
		final DelaySimpleTask delayTask = new DelaySimpleTask("hello");
		delayTask.setKey(10);
		simpleTimeWheel.addTask(delayTask);

		final DelaySimpleTask delayTask2 = new DelaySimpleTask("hello ___2");
		delayTask2.setKey(20);
		simpleTimeWheel.addTask(delayTask2);
		simpleTimeWheel.start();

	}

	private static class DelaySimpleTask extends SimpleTask {
		private String msg;

		public DelaySimpleTask(String msg) {
			this.msg = msg;
		}

		@Override
		public void run() {
			log.info("delay msg " + this.msg);
		}
	}
}
