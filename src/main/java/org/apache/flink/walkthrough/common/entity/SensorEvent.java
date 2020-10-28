package org.apache.flink.walkthrough.common.entity;

import com.google.common.base.Objects;

/**
 * @author zhang lianhui
 * @date 2020/10/28 10:51 上午
 */
public class SensorEvent {
	private String id;
	private Long timestamp;
	private Double temperature;

	public SensorEvent(String id, Long timestamp, Double temperature) {
		this.id = id;
		this.timestamp = timestamp;
		this.temperature = temperature;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SensorEvent that = (SensorEvent) o;
		return Objects.equal(id, that.id) &&
				Objects.equal(timestamp, that.timestamp) &&
				Objects.equal(temperature, that.temperature);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id, timestamp, temperature);
	}

	@Override
	public String toString() {
		return "SensorEvent{" +
				"id='" + id + '\'' +
				", timestamp=" + timestamp +
				", temperature=" + temperature +
				'}';
	}
}
