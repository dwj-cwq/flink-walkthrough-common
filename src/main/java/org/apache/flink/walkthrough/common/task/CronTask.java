package org.apache.flink.walkthrough.common.task;

import com.google.common.base.Objects;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.time.DateFormatUtils;

import org.apache.flink.walkthrough.common.entity.Rule;
import org.apache.flink.walkthrough.common.timewheel.CronExpression;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTask;
import org.apache.flink.walkthrough.common.util.TimeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zhang lianhui
 * @date 2020/11/11 10:42 上午
 */
public class CronTask extends TimerTask implements Serializable {
	private static final long serialVersionUID = 3380958011711529982L;
	private static final Logger log = LoggerFactory.getLogger(CronTask.class);
	private final String id;
	private final String cronExpression;
	private volatile boolean cancel = false;
	private final LinkedBlockingDeque<Rule> queue;
	private long lastExecuteTimestamp = -1;

	public CronTask(
			String id,
			String cronExpress,
			LinkedBlockingDeque<Rule> queue) {
		this.id = id;
		this.cronExpression = cronExpress;
		this.queue = queue;
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
		if (lastExecuteTimestamp == -1) {
			lastExecuteTimestamp = System.currentTimeMillis();
		}

		final Date lastExecDate = timestampToDate(lastExecuteTimestamp);
		final CronExpression expression = CronCacheFactory
				.getCronCache()
				.get(this.cronExpression);
		final Date nextExecuteDate = expression.getNextValidTimeAfter(lastExecDate);
		final long nextTime = dateToTimestamp(nextExecuteDate);
		final long delay = nextTime - System.currentTimeMillis();
		this.setDelayMs(delay);
		lastExecuteTimestamp = nextTime;
	}

	@Override
	public void run() {
		// log.info("cron task run");
		// 生成 rule
		produceMonitorTriggerRecord(this);
	}

	public static void produceMonitorTriggerRecord(CronTask task) {
		Rule rule = new Rule(
				task.getId(),
				600L,
				System.currentTimeMillis());
		task.queue.addLast(rule);
		if (!task.isCancel()) {
			TaskManagerFactory.getTaskManager().add(task);
		} else {
			TaskManagerFactory.getTaskManager().delete(task.getId());
		}
	}

	@Override
	public String toString() {
		return "CronTask{" +
				"id='" + id + '\'' +
				", cronExpression=" + cronExpression +
				", cancel=" + cancel +
				", lastExecuteDate=" + TimeUtil.epochMilliFormat(lastExecuteTimestamp) +
				'}';
	}

	public static Date timestampToDate(long timestamp) {
		return new Date(timestamp);
	}

	public static long dateToTimestamp(Date date) {
		return date.toInstant().toEpochMilli();
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
				Objects.equal(lastExecuteTimestamp, cronTask.lastExecuteTimestamp);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id, cronExpression, cancel, lastExecuteTimestamp);
	}
}
