package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

/**
 * @author kafka
 * @date 2020/11/9 10:44 上午
 */
public abstract class Timer {

	/**
	 * 添加一个新的task 到 executor， 当达到 task 的延迟 时间时（任务提交时被计时）， 该任务会被执行
	 *
	 * @param timerTask 新添加的任务
	 */
	public abstract void add(TimerTask timerTask);

	/**
	 * Advance the internal clock, executing any tasks whose expiration has been
	 * reached within the duration of the passed timeout.
	 *
	 * @param timeoutMs
	 *
	 * @return whether or not any tasks were executed
	 */
	public abstract boolean advanceClock(long timeoutMs);

	/**
	 * get the number of tasks pending execution
	 *
	 * @return the number of tasks
	 */
	public abstract int getSize();

	/**
	 * shutdown the timer service, leaving pending tasks unexecuted
	 */
	public abstract void shutdown();
}
