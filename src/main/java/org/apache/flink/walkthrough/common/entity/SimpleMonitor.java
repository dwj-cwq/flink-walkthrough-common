package org.apache.flink.walkthrough.common.entity;

/**
 * @author zhang lianhui
 * @date 2020/10/29 7:49 下午
 */
public class SimpleMonitor implements Monitor {
	private String monitorId;
	private String cronExpression;

	public SimpleMonitor(String monitorId, String cronExpression) {
		this.monitorId = monitorId;
		this.cronExpression = cronExpression;
	}

	@Override
	public String getMonitorId() {
		return this.monitorId;
	}

	public void setMonitorId(String monitorId) {
		this.monitorId = monitorId;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	@Override
	public String getCronExpression() {
		return this.cronExpression;
	}


}
