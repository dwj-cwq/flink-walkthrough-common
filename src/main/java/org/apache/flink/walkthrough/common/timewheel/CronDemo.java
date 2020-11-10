package org.apache.flink.walkthrough.common.timewheel;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

/**
 * @author zhang lianhui
 * @date 2020/11/7 5:19 下午
 */
public class CronDemo {
	public static Map<String, CronExpression> cronExpressionMap = Maps.newConcurrentMap();

	public static void main(String[] args) throws ParseException {

		String cron = "0/10 * * * * ?";
		CronExpression cronExpression = cronExpressionMap.get(cron);
		if (cronExpression == null) {
			cronExpression = new CronExpression(cron);
			cronExpressionMap.put(cron, cronExpression);
		}
		Date current = new Date();
		for (int i = 0; i < 10; i++) {
			String currentStr = DateFormatUtils.format(current, "yyyy-MM-dd HH:mm:ss");
			System.out.println("current :" + currentStr);
			current = cronExpression.getNextValidTimeAfter(current);
		}
	}
}
