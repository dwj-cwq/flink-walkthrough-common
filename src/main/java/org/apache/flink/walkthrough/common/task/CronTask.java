package org.apache.flink.walkthrough.common.task;

import com.google.common.base.Objects;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.time.DateFormatUtils;

import org.apache.flink.walkthrough.common.entity.Monitor;
import org.apache.flink.walkthrough.common.entity.Rule;
import org.apache.flink.walkthrough.common.timewheel.CronExpression;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTask;
import org.apache.flink.walkthrough.common.util.Util;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zhang lianhui
 * @date 2020/11/11 10:42 上午
 */
@Slf4j
public class CronTask extends TimerTask implements Serializable {
	private static final long serialVersionUID = 3380958011711529982L;
	private final String id;
	private final CronExpression cronExpression;
	private volatile boolean cancel = false;
	private final TaskManager taskManager = TaskManagerFactory.getTaskManager();
	private final LinkedBlockingDeque<Rule> queue;
	private final Monitor monitor;
	private Date lastExecuteDate;

	public CronTask(
			String id,
			LinkedBlockingDeque<Rule> queue,
			Monitor monitor) {
		this.id = id;
		this.cronExpression = Util.parseCron(monitor.getCronExpression());
		this.queue = queue;
		this.monitor = monitor;
	}

	public CronExpression getCronExpression() {
		return cronExpression;
	}

	public boolean isCancel() {
		return cancel;
	}

	public String getId() {
		return id;
	}

	@Override
	public void cancel() {
		cancel = true;
		super.cancel();
	}

	public void updateDelayMs() {
		if (lastExecuteDate == null) {
			lastExecuteDate = new Date();
		}
		final Date nextExecuteDate = cronExpression.getNextValidTimeAfter(lastExecuteDate);
		final long nextTime = nextExecuteDate.toInstant().toEpochMilli();
		final long delay = nextTime - System.currentTimeMillis();
		this.setDelayMs(delay);
		lastExecuteDate = nextExecuteDate;
	}

	@Override
	public void run() {
		// log.info("cron task run");
		// 生成 rule
		queue.addLast(new Rule(
				monitor.getMonitorId(),
				monitor.getCronExpression(),
				monitor.getDelta(),
				System.currentTimeMillis()));
		if (!isCancel()) {
			taskManager.add(this);
		} else {
			taskManager.delete(id);
		}
	}

	@Override
	public String toString() {
		return "CronTask{" +
				"id='" + id + '\'' +
				", cronExpression=" + cronExpression +
				", cancel=" + cancel +
				", lastExecuteDate=" + getLastExecuteDate(lastExecuteDate) +
				'}';
	}

	public String getLastExecuteDate(Date lastExecuteDate) {
		if (lastExecuteDate == null) {
			return null;
		}
		return DateFormatUtils.format(lastExecuteDate, "yyyy-MM-dd HH:mm:ss");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CronTask cronTask = (CronTask) o;
		return Objects.equal(id, cronTask.id) &&
				Objects.equal(cronExpression, cronTask.cronExpression) &&
				Objects.equal(cancel, cronTask.cancel) &&
				Objects.equal(monitor, cronTask.monitor) &&
				Objects.equal(lastExecuteDate, cronTask.lastExecuteDate);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id, cronExpression, cancel, monitor, lastExecuteDate);
	}
}
