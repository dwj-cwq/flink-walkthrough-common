package org.apache.flink.walkthrough.common.entity;

/**
 * @author zhang lianhui
 * @date 2020/10/29 7:49 下午
 */
public class SimpleMonitor implements Monitor {
	private String monitorId;
	private String cronExpression;
	private Long delta;

	public SimpleMonitor() {

	}

	public SimpleMonitor(String monitorId, String cronExpression, Long delta) {
		this.monitorId = monitorId;
		this.cronExpression = cronExpression;
		this.delta = delta;
	}

	@Override
	public String getMonitorId() {
		return this.monitorId;
	}


	@Override
	public Long getDelta() {
		return this.delta;
	}

	@Override
	public String getCronExpression() {
		return this.cronExpression;
	}

	public void setMonitorId(String monitorId) {
		this.monitorId = monitorId;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public void setDelta(Long delta) {
		this.delta = delta;
	}


}
