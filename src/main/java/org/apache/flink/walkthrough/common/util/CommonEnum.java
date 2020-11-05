package org.apache.flink.walkthrough.common.util;

import com.google.common.base.Enums;

/**
 * @author zhang lianhui
 * @date 2020/11/2 7:59 下午
 */
public interface CommonEnum {
	static <T extends Enum<T>> T getIfPresent(final String type, Class<T> clazz) {
		if (type == null) {
			return null;
		}
		return Enums.getIfPresent(clazz, type.toUpperCase()).orNull();
	}

	static <T extends Enum<T>> T getIfPresent(final String type, Class<T> clazz, T dft) {
		if (type == null) {
			return null;
		}
		return Enums.getIfPresent(clazz, type.toUpperCase()).or(dft);
	}
}
