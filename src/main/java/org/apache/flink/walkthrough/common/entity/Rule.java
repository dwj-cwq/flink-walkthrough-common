package org.apache.flink.walkthrough.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhang lianhui
 * @date 2020/11/5 11:22 上午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Rule extends SimpleMonitor {
	private String monitorId;
	private Long delta;
	private Long timestamp;

	public Long getCutOffTimestamp() {
		return this.timestamp - this.delta;
	}
}
