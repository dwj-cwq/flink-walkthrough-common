package org.apache.flink.walkthrough.common.task;

import org.apache.flink.walkthrough.common.timewheel.CronExpression;
import org.apache.flink.walkthrough.common.util.Util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhang lianhui
 * @date 2020/11/12 3:49 下午
 */
public class CronCache {
	private Map<String, CronExpression> cacheMap = new ConcurrentHashMap<>();

	public CronExpression get(String cronExpression) {
		CronExpression expression;
		if (!cacheMap.containsKey(cronExpression)) {
			expression = Util.parseCron(cronExpression);
			cacheMap.put(cronExpression, expression);
		} else {
			expression = cacheMap.get(cronExpression);
		}
		return expression;
	}
}
