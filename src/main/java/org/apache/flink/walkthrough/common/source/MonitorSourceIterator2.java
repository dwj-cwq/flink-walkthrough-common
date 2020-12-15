package org.apache.flink.walkthrough.common.source;

import org.apache.flink.walkthrough.common.entity.Monitor;
import org.apache.flink.walkthrough.common.entity.Rule;
import org.apache.flink.walkthrough.common.entity.SimpleMonitor;
import org.apache.flink.walkthrough.common.task.CronTask;
import org.apache.flink.walkthrough.common.task.TaskManager;
import org.apache.flink.walkthrough.common.task.TaskManagerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zhang lianhui
 * @date 2020/11/11 3:15 下午
 */
public class MonitorSourceIterator2 implements Iterator<Rule>, Serializable {
	private static final long serialVersionUID = -3326297488411505913L;
	private static LinkedBlockingDeque<Rule> deque = new LinkedBlockingDeque<>();
	private final transient TaskManager taskManager = TaskManagerFactory.getTaskManager();

	@Override
	public boolean hasNext() {
		return true;
	}

	public MonitorSourceIterator2 init() {
		final List<Monitor> allMonitors = findAllMonitors();
		for (Monitor monitor : allMonitors) {
			final CronTask cronTask = new CronTask(
					monitor.getMonitorId(),
					monitor.getCronExpression(),
					deque);
			taskManager.add(cronTask);
		}
		return this;
	}

	@Override
	public Rule next() {
		while (true) {
			System.out.printf("thread: %s", Thread.currentThread().getName());
			if (!deque.isEmpty()) {
				return deque.pollFirst();
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}


	private List<Monitor> findAllMonitors() {
		List<Monitor> monitors = new ArrayList<>();
		monitors.add(new SimpleMonitor("monitor_a", "0/20 * * * * ? "));
		monitors.add(new SimpleMonitor("monitor_b", "0 0/1 * * * ? "));
		return monitors;
	}

//	private List<Monitor> findAllMonitors() {
//		List<Monitor> monitors = new ArrayList<>();
//		for (int i = 0; i < 50_0000; i++) {
//			int seconds = 5 * ((i % 11) + 1);
//			String cronExpression = String.format("0/%d * * * * ? ", seconds);
//			monitors.add(new SimpleMonitor("monitor_" + i, cronExpression));
//		}
//		return monitors;
//	}

}
