package org.apache.flink.walkthrough.common.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import org.apache.flink.walkthrough.common.util.TimeUtil;

/**
 * @author zhang lianhui
 * @date 2020/10/29 3:20 下午
 */
@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordImp implements Record {
	private String monitorId;
	private Long timestamp;
	private Double value;
	public static final String DEFAULT_TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";

	@Override
	public String toString() {
		return "RecordImp{" +
				"monitorId='" + monitorId + '\'' +
				", timestamp=" + TimeUtil.epochMilliFormat(this.timestamp, TimeUtil.DEFAULT_TS_PATTERN) +
				", value=" + value +
				'}';
	}
}
