package org.apache.flink.walkthrough.common.source;

import org.apache.flink.walkthrough.common.entity.Monitor;
import org.apache.flink.walkthrough.common.entity.Rule;
import org.apache.flink.walkthrough.common.entity.SimpleMonitor;
import org.apache.flink.walkthrough.common.util.JobUtil;

import org.quartz.SchedulerException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author zhang lianhui
 * @date 2020/10/30 11:06 上午
 */
public class MonitorSourceIterator implements Iterator<Rule>, Serializable {
	private static final long serialVersionUID = 8381054071358505226L;
	private static LinkedBlockingDeque<Rule> linkedBlockingDeque = new LinkedBlockingDeque<>(
			1000 * 1000);

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public Rule next() {
		for (; ; ) {
			if (!linkedBlockingDeque.isEmpty()) {
				return linkedBlockingDeque.pollFirst();
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	static {
		final List<Monitor> allMonitors = findAllMonitors();
		for (Monitor monitor : allMonitors) {
			try {
				JobUtil.createJob(monitor, linkedBlockingDeque);
			} catch (SchedulerException e) {
				e.printStackTrace();
			}
		}
	}

	private static List<Monitor> findAllMonitors() {
		List<Monitor> monitors = new ArrayList<>();
		monitors.add(new SimpleMonitor("monitor_a", "0/20 * * * * ? ", 60 * 1000L));
		monitors.add(new SimpleMonitor("monitor_b", "0 0/1 * * * ? ", 10 * 1000L));
		return monitors;
	}
//
//	private static List<Monitor> findAllMonitors() {
//		List<Monitor> monitors = new ArrayList<>();
//		for (int i = 0; i < 100000; i++) {
//			int seconds = 5 * ((i % 11) + 1);
//			String cronExpression = String.format("0/%d * * * * ? ", seconds);
//			monitors.add(new SimpleMonitor("monitor_" + i, cronExpression));
//		}
//		return monitors;
//	}

}
