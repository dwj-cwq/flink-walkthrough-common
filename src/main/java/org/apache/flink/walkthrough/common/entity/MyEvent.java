package org.apache.flink.walkthrough.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author zhang lianhui
 * @date 2020/10/28 3:17 下午
 */
@AllArgsConstructor
@Data
public class MyEvent {
	private String user;
	private Long timestamp;
	private Double value;
}
