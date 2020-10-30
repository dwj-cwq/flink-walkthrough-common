package org.apache.flink.walkthrough.common.entity;

/**
 * @author zhang lianhui
 * @date 2020/10/29 7:45 下午
 */
public interface Monitor {
	String getMonitorId();
	default String getCronExpression() {
		// 监控粒度，最细单位为 s，参考 Quartz 表达式规范
		// http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html
		return "0 * * * * ?";   // 默认每分钟 0s 触发
	}
}
