package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import com.google.common.base.Objects;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.walkthrough.common.util.TimeUtil;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.lang.Long.*;


/**
 * TimerTaskList 双向循环链表
 *
 * @author kafka
 * @date 2020/11/9 10:47 上午
 */
@Slf4j
public class TimerTaskList implements Delayed {
	/**
	 * 虚拟节点
	 * root.next 指向 head 节点
	 * root.prev 指向 tail 节点
	 */
	private TimerTaskEntry root;

	/**
	 * 任务计数器
	 */
	private AtomicInteger taskCounter;

	/**
	 * 桶的过期时间
	 */
	private AtomicLong expiration = new AtomicLong(-1);


	public TimerTaskList(AtomicInteger taskCounter) {
		this.taskCounter = taskCounter;
		root = new TimerTaskEntry(null, -1);
		root.next = root;
		root.prev = root;
	}

	/**
	 * @param expirationMs Set the bucket's expiration time
	 *
	 * @return Returns true if the expiration time is changed
	 */
	public boolean setExpiration(long expirationMs) {
		return expiration.getAndSet(expirationMs) != expirationMs;
	}

	/**
	 * @return get the bucket's expiration time
	 */
	public Long getExpiration() {
		return expiration.get();
	}

	/**
	 * @param timerTaskEntry add a timer task entry to this list
	 */
	public void add(TimerTaskEntry timerTaskEntry) {
		boolean done = false;
		while (!done) {
			// Remove the timer task entry if it is already in any other list
			// We do this outside of the sync block below to avoid deadlocking.
			// We may retry until timerTaskEntry.list becomes null.
			timerTaskEntry.remove();
			synchronized (this) {
				if (timerTaskEntry.list == null) {
					// put the timer task entry to the end of the list. (root.prev points to the tail entry)
					final TimerTaskEntry tail = root.prev;
					timerTaskEntry.next = root;
					timerTaskEntry.prev = tail;
					timerTaskEntry.list = this;
					tail.next = timerTaskEntry;
					root.prev = timerTaskEntry;
					taskCounter.incrementAndGet();
					done = true;
				}
			}

		}
	}

	/**
	 * @param taskEntry Remove the specified timer task entry from this list
	 */
	public synchronized void remove(TimerTaskEntry taskEntry) {
		if (taskEntry.list == this) {
			taskEntry.next.prev = taskEntry.prev;
			taskEntry.prev.next = taskEntry.next;
			taskEntry.next = null;
			taskEntry.prev = null;
			taskEntry.list = null;
			taskCounter.decrementAndGet();
		}
	}

	/**
	 * @param function 对链表中的所有 TimerTask 应用某个函数
	 */
	public synchronized void foreach(Consumer<TimerTask> function) {
		TimerTaskEntry curr = root.next;
		while (curr != root) {
			TimerTaskEntry next = curr.next;
			if (!curr.cancelled()) {
				function.accept(curr.getTimerTask());
				curr = next;
			}
		}
	}

	/**
	 * @param function Remove all task entries and apply the supplied function to each of them
	 */
	public synchronized void flush(Consumer<TimerTaskEntry> function) {
		TimerTaskEntry head = root.next;
		while (head != root && head != null) {
			remove(head);
			function.accept(head);
			head = root.next;
		}
		expiration.set(-1L);
	}

	/**
	 * currentTimeMillis()方法的时间精度依赖于操作系统的具体实现，有些操作系统下并不能达到毫秒级的精度
	 * 这里 采用了System.nanoTime()/1_000_000来将精度调整到毫秒级。
	 *
	 * @param timeUnit
	 *
	 * @return
	 */
	@Override
	public long getDelay(TimeUnit timeUnit) {

		final long convert = timeUnit.convert(
				max(getExpiration() - TimeUtil.hiResClockMs(), 0),
				TimeUnit.MILLISECONDS);
		// log.info("current expiration: {}, get delay: {}", getExpiration(), convert);
		return convert;
	}

	@Override
	public int compareTo(Delayed delayed) {
		TimerTaskList other = (TimerTaskList) delayed;
		return Long.compare(getExpiration(), other.getExpiration());
	}

	@Override
	public String toString() {
		return "TimerTaskList{" +
				"taskCounter=" + taskCounter +
				", expiration=" + expiration +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TimerTaskList that = (TimerTaskList) o;
		return Objects.equal(taskCounter, that.taskCounter) &&
				Objects.equal(expiration, that.expiration);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(taskCounter, expiration);
	}

	public static class TimerTaskEntry implements Comparable<TimerTaskEntry> {
		private volatile TimerTaskList list;
		private TimerTaskEntry prev;
		private TimerTaskEntry next;
		private TimerTask timerTask;
		private long expirationMs;

		public TimerTaskList getList() {
			return list;
		}

		public void setList(TimerTaskList list) {
			this.list = list;
		}

		public TimerTask getTimerTask() {
			return timerTask;
		}

		public void setTimerTask(TimerTask timerTask) {
			this.timerTask = timerTask;
		}

		public long getExpirationMs() {
			return expirationMs;
		}

		public void setExpirationMs(long expirationMs) {
			this.expirationMs = expirationMs;
		}

		public TimerTaskEntry(TimerTask timerTask, long expirationMs) {
			this.expirationMs = expirationMs;
			this.timerTask = timerTask;

			// if this timerTask is already held by an existing timer task entry,
			// setTimerTaskEntry will remove it.
			if (this.timerTask != null) {
				this.timerTask.setTimerTaskEntry(this);
			}
		}

		/**
		 * 该任务是否被取消
		 *
		 * @return
		 */
		public boolean cancelled() {
			return timerTask.getTimerTaskEntry() != this;
		}

		/**
		 * 从所属的双向链表中删除此 TimerTaskEntry
		 */
		public void remove() {
			TimerTaskList currentList = list;
			// If remove is called when another thread is moving the entry from a task entry list to another,
			// this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
			// In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
			while (currentList != null) {
				currentList.remove(this);
				currentList = list;
			}
		}

		@Override
		public int compareTo(TimerTaskEntry other) {
			return Long.compare(this.expirationMs, other.expirationMs);
		}

		@Override
		public String toString() {
			return "TimerTaskEntry{" +
					"timerTask=" + timerTask +
					", expirationMs=" + expirationMs +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TimerTaskEntry that = (TimerTaskEntry) o;
			return expirationMs == that.expirationMs &&
					Objects.equal(timerTask, that.timerTask);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(timerTask, expirationMs);
		}
	}
}
