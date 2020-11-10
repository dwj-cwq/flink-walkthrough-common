package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;


import static org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTaskList.TimerTaskEntry;


/**
 * @author kafka
 * @date 2020/11/9 10:46 上午
 */
public abstract class TimerTask implements Runnable {
	// timestamp in millisecond
	private long delayMs;

	private TimerTaskEntry timerTaskEntry;

	public long getDelayMs() {
		return delayMs;
	}

	public void setDelayMs(long delayMs) {
		this.delayMs = delayMs;
	}

	public synchronized void cancel() {
		if (timerTaskEntry != null) {
			timerTaskEntry.remove();
			timerTaskEntry = null;
		}
	}

	public synchronized void setTimerTaskEntry(TimerTaskEntry entry) {
		if (timerTaskEntry != null && timerTaskEntry != entry) {
			timerTaskEntry.remove();
		}
		timerTaskEntry = entry;
	}

	public TimerTaskEntry getTimerTaskEntry() {
		return this.timerTaskEntry;
	}

	@Override
	public String toString() {
		return "TimerTask{" +
				"delayMs=" + delayMs +
				'}';
	}
}
