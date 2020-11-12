package org.apache.flink.walkthrough.common.entity;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.walkthrough.common.util.JobUtil;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zhang lianhui
 * @date 2020/10/30 11:31 上午
 */
@Slf4j
public class MonitorJob implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		final JobDetail jobDetail = context.getJobDetail();
		final LinkedBlockingDeque<Rule> deque = (LinkedBlockingDeque<Rule>) jobDetail
				.getJobDataMap()
				.get(JobUtil.QUEUE);
		final Monitor monitor = (Monitor) jobDetail.getJobDataMap().get(JobUtil.MONITOR);
		deque.addLast(new Rule(
				monitor.getMonitorId(),
				monitor.getCronExpression(),
				monitor.getDelta(),
				System.currentTimeMillis()));
	}
}
