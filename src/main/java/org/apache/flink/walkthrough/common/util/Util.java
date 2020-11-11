package org.apache.flink.walkthrough.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.walkthrough.common.timewheel.CronExpression;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author zhang lianhui
 * @date 2020/4/16 12:26 上午
 */
@SuppressWarnings("unused")
@Slf4j
public class Util {
	static final int MAXIMUM_CAPACITY = 1 << 30;

	public static boolean nonBlankString(String string) {
		return !Strings.isNullOrEmpty(string);
	}

	public static String convertWildcardToRegex(String wildcard) {
		return String.format("%s", wildcard.replace(".", "\\.").replace("*", ".*"));
	}

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(
			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

	public static String obj2json(Object obj) {
		try {
			return OBJECT_MAPPER.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T json2obj(String json, Class<T> targetType) {
		try {
			return OBJECT_MAPPER.readValue(json, targetType);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T json2obj(String json, TypeReference<T> valueTypeRef) {
		try {
			return OBJECT_MAPPER.readValue(json, valueTypeRef);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Map<String, Object> obj2map(Object obj) {
		if (obj == null) {
			return null;
		}
		return json2obj(obj2json(obj), HashMap.class);
	}

	public static <T> T map2obj(Map map, Class<T> targetType) {
		if (map == null) {
			return null;
		}
		return json2obj(obj2json(map), targetType);
	}

	public static <T> T map2obj(Object obj, Class<T> targetType) {
		if (obj != null && (!(obj instanceof Map<?, ?>))) {
			return null;
		}
		return map2obj((Map<String, Object>) obj, targetType);
	}

	public static boolean isValidPeriod(Long[] timeRange) {
		return timeRange != null && timeRange.length == 2 && timeRange[0] <= timeRange[1];
	}

	public static JsonNode getJsonNode(String json) {
		try {
			return OBJECT_MAPPER.readTree(json);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String escapeChar(String key) {
		return key.replace("\"", "\\\"");
	}

	public static String encode(String string) {
		try {
			return URLEncoder.encode(string, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance();

	static {
		NUMBER_FORMAT.setMaximumFractionDigits(2);
		NUMBER_FORMAT.setGroupingUsed(false);
	}

	/**
	 * @return 最多保留两位小数
	 */
	public static String numberFormat(double value) {
		return NUMBER_FORMAT.format(value);
	}

	public static String doubleFormat(double d) {
		return (d == (long) d) ? String.format("%d", (long) d) : String.format("%s", d);
	}

	public static String getTmpDir() {
		return System.getProperty("java.io.tmpdir");
	}

	public static <T, U> List<U> mapTo(
			List<T> typesStr,
			Function<? super T, ? extends U> converter) {
		List<U> results = Collections.emptyList();
		if (typesStr != null) {
			results = typesStr
					.stream()
					.map(converter)
					.filter(Objects::nonNull)
					.collect(Collectors.toList());
		}
		return results;
	}

	public static String extract(String str, Pattern pattern) {
		Matcher matcher = pattern.matcher(str);
		boolean find = matcher.find();
		if (!find || matcher.groupCount() < 1) {
			return "";
		}
		return matcher.group(1);
	}

	public static String getUuid() {
		return UUID.randomUUID().toString();
	}

	/**
	 * Returns a power of two size for the given target capacity.
	 */
	public static int tableSizeFor(int cap) {
		int n = cap - 1;
		n |= n >>> 1;
		n |= n >>> 2;
		n |= n >>> 4;
		n |= n >>> 8;
		n |= n >>> 16;
		return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
	}

	public static CronExpression parseCron(String cronExpression) {
		try {
			return new CronExpression(cronExpression);
		} catch (ParseException pe) {
			log.error(
					"parse cron exception, cron {}, error message: {}",
					cronExpression,
					pe.getMessage(),
					pe);
		}
		return null;
	}
}
