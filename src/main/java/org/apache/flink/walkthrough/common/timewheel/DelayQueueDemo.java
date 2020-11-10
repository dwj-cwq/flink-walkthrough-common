package org.apache.flink.walkthrough.common.timewheel;

import com.google.common.base.Objects;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author zhang lianhui
 * @date 2020/11/7 11:29 上午
 */
@Slf4j
public class DelayQueueDemo {
	public static void main(String[] args) {
		DelayQueue<MyDelay> delayQueue = new DelayQueue<>();
		log.info("start ");


		for (int i = 0; i < 10; i++) {
			delayQueue.add(new MyDelay(i, (10 - i)));
		}

		while (!delayQueue.isEmpty()) {
			final MyDelay curr = delayQueue.poll();
			if (curr != null) {
				log.info("curr: " + curr);
			}
		}

	}


	public static class MyDelay implements Delayed {
		private int id;

		private long delaySeconds;

		private long currentTime;

		public MyDelay(int id, long delaySeconds) {
			this.delaySeconds = delaySeconds;
			this.id = id;
			this.currentTime = System.currentTimeMillis();
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public long getDelaySeconds() {
			return delaySeconds;
		}

		public void setDelaySeconds(long delaySeconds) {
			this.delaySeconds = delaySeconds;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			MyDelay myDelay = (MyDelay) o;
			return id == myDelay.id &&
					delaySeconds == myDelay.delaySeconds;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(id, delaySeconds);
		}

		// 计算剩余的过期时间
		// 大于 0 说明未过期
		@Override
		public long getDelay(TimeUnit unit) {
			return delaySeconds - TimeUnit.MILLISECONDS.toSeconds( System.currentTimeMillis() - this.currentTime);
		}

		@Override
		public int compareTo(Delayed o) {
			return Long.compare(this.delaySeconds, o.getDelay(TimeUnit.SECONDS));
		}

		@Override
		public String toString() {
			return "MyDelay{" +
					"id=" + id +
					", delaySeconds=" + delaySeconds +
					'}';
		}
	}

}
