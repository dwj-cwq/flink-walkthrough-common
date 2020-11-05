package org.apache.flink.walkthrough.common.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Strings;

import org.apache.flink.walkthrough.common.util.CommonEnum;

/**
 * @author zhang lianhui
 * @date 2020/11/2 3:51 下午
 */
public enum RecordType {
	MONITOR("monitor"),
	REAL_RECORD("real_record");

	private String type;


	RecordType(String type) {
		this.type = type;
	}

	@JsonValue
	@Override
	public String toString() {
		return this.type;
	}


	@JsonCreator
	public static RecordType getIfPresent(final String name) {
		if (Strings.isNullOrEmpty(name)) {
			return REAL_RECORD;
		}
		return CommonEnum.getIfPresent(name.replace("-", "_"), RecordType.class);
	}

}
