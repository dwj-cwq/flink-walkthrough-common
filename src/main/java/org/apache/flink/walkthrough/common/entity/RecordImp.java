package org.apache.flink.walkthrough.common.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.flink.walkthrough.common.util.TimeUtil;

/**
 * @author zhang lianhui
 * @date 2020/10/29 3:20 下午
 */
@Data
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
public class RecordImp implements Record {
	@JsonProperty("monitor_id")
	private String monitorId;

	private Long timestamp;
	private Double value;

	public static final String DEFAULT_TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";

	@Override
	public String toString() {
		final Object tv = timestamp == null ? timestamp :
				TimeUtil.epochMilliFormat(this.timestamp, TimeUtil.DEFAULT_TS_PATTERN);
		return "RecordImp{" +
				"monitorId='" + monitorId + '\'' +
				", timestamp=" + tv +
				", value=" + value +
				'}';
	}
}
