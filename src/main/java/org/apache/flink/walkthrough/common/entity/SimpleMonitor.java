package org.apache.flink.walkthrough.common.entity;

import com.google.common.base.Objects;

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

	@Override
	public String toString() {
		return "SimpleMonitor{" +
				"monitorId='" + monitorId + '\'' +
				", cronExpression='" + cronExpression + '\'' +
				", delta=" + delta +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SimpleMonitor that = (SimpleMonitor) o;
		return Objects.equal(monitorId, that.monitorId) &&
				Objects.equal(cronExpression, that.cronExpression) &&
				Objects.equal(delta, that.delta);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(monitorId, cronExpression, delta);
	}
}
