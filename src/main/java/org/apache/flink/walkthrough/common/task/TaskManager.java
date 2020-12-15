package org.apache.flink.walkthrough.common.task;


import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.SystemTimer;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.Timer;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimingWheelExpirationService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhang lianhui
 * @date 2020/11/11 11:13 上午
 */
public class TaskManager implements TaskManagerOperation<CronTask, String>, Serializable {
	private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

	private static final long serialVersionUID = 6125129130305409210L;
	private static final long TICK_MS = 1;
	private static final int WHEEL_SIZE = 60;
	private final long tickMs;
	private final int wheelSize;
	private transient Timer timer;
	private transient TimingWheelExpirationService expirationService;

	public TaskManager() {
		this(TICK_MS, WHEEL_SIZE);
	}

	public TaskManager(long tickMs, int wheelSize) {
		this.tickMs = tickMs;
		this.wheelSize = wheelSize;
	}

	public void init() {
		timer = new SystemTimer(
				"Task manager timer",
				tickMs,
				wheelSize);
		expirationService = new TimingWheelExpirationService(timer);
		expirationService.start();
	}

	public void shutdown() {
		if (expirationService != null) {
			expirationService.shutdown();
		}
	}

	private final Map<String, CronTask> PLATFORM = new ConcurrentHashMap<>();

	public Timer getTimer() {
		return timer;
	}

	@Override
	public CronTask add(CronTask task) {
		PLATFORM.put(task.getId(), task);
		// 更新延迟时间
		if (!task.isCancel()) {
			task.updateDelayMs();
		}
		// 添加任务到时间轮
		timer.add(task);
		// log.info("timing wheel size: {}, new task: {} ", timer.getSize(), task);
		return task;
	}

	@Override
	public boolean delete(String id) {
		if (!PLATFORM.containsKey(id)) {
			return false;
		}

		// 取消任务
		cancel(id);

		PLATFORM.remove(id);
		return true;
	}

	@Override
	public CronTask find(String id) {
		return PLATFORM.get(id);
	}

	@Override
	public boolean cancel(String id) {
		final CronTask timerTask = find(id);
		if (timerTask == null) {
			return false;
		}
		timerTask.cancel();
		return true;
	}

	@Override
	public void clear() {
		PLATFORM.clear();
	}
}
