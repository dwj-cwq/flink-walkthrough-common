package org.apache.flink.walkthrough.common.util;

import org.apache.flink.walkthrough.common.entity.Monitor;
import org.apache.flink.walkthrough.common.entity.MonitorJob;

import org.apache.flink.walkthrough.common.entity.Record;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zhang lianhui
 * @date 2020/10/30 11:21 上午
 */
public class JobUtil {
	private static volatile Scheduler scheduler;
	private static final String DEFAULT_GROUP = "monitor";
	public static final String QUEUE = "queue";

	static {
		try {
			init();
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	private static synchronized void init() throws SchedulerException {
		if (scheduler == null) {
			scheduler = new StdSchedulerFactory().getScheduler();
			scheduler.start();
		}
	}

	public static String createJob(
			Monitor monitor,
			LinkedBlockingDeque<Record> deque) throws SchedulerException {
		final String monitorId = monitor.getMonitorId();
		JobKey jobKey = new JobKey(monitorId, DEFAULT_GROUP);
		JobDetail jobDetail = JobBuilder.newJob(MonitorJob.class).withIdentity(jobKey).build();
		jobDetail.getJobDataMap().put(QUEUE, deque);
		final Trigger trigger = TriggerBuilder.newTrigger()
				.withIdentity(monitorId, DEFAULT_GROUP)
				.withSchedule(cron(monitor.getCronExpression()))
				.build();

		scheduler.scheduleJob(jobDetail, trigger);
		return monitorId;
	}

	private static ScheduleBuilder<CronTrigger> cron(String cronExpression) {
		return CronScheduleBuilder.cronSchedule(cronExpression);
	}
}
