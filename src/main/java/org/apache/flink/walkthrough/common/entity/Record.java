package org.apache.flink.walkthrough.common.entity;

/**
 * @author zhang lianhui
 * @date 2020/10/29 2:53 下午
 */
public interface Record {
	String getMonitorId();
	Long getTimestamp();
	Double getValue();
	RecordType getRecordType();
}
