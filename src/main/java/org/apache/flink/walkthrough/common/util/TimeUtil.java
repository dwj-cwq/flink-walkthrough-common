package org.apache.flink.walkthrough.common.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author zhang lianhui
 * @date 2020/10/29 5:28 下午
 */

@SuppressWarnings("unused")
public class TimeUtil {
	public static final String DEFAULT_TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS Z";

	public static long parseTimeWithFormat(
			String time,
			String format,
			int unit) throws ParseException {
		if (unit <= 0) {
			return 0L;
		}
		return parseTimeWithFormat(time, format) / unit;
	}

	public static long parseTimeWithFormat(String time, String format) throws ParseException {
		return new SimpleDateFormat(format).parse(time).getTime();
	}

	private static long parseTimeWithFormat(
			String time,
			String format,
			String timezone) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sdf.setTimeZone(TimeZone.getTimeZone(timezone));
		return sdf.parse(time).getTime() / 1000;
	}

	public static long parseUtcTime(String time) throws ParseException {
		return parseTimeWithFormat(time.substring(0, 19), "yyyy-MM-dd'T'HH:mm:ss", "GMT");
	}

	public static long parseRequestTime(String time) throws ParseException {
		if (time.length() == 6) {
			return parseTimeWithFormat(time, "yyyyMM", 1000);
		} else if (time.length() == 8) {
			return parseTimeWithFormat(time, "yyyyMMdd", 1000);
		} else {
			return Long.parseLong(time);
		}
	}

	public static LocalDateTime epochMillisParseUtc(Long timestamp) {
		return LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("UTC"));
	}

	private static final DateTimeFormatter ES_TIME_FORMATTER = DateTimeFormatter
			.ofPattern(DEFAULT_TS_PATTERN);

	public static ZonedDateTime parse(String dateStr, DateTimeFormatter dateTimeFormatter) {
		return ZonedDateTime.parse(dateStr, dateTimeFormatter);
	}

	public static ZonedDateTime parseMillis(Long timestamp) {
		timestamp = timestamp.toString().length() == 10 ? timestamp * 1000 : timestamp;
		return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
	}

	public static String format(LocalDateTime date, DateTimeFormatter dateTimeFormatter) {
		return dateTimeFormatter.format(date);
	}

	public static long epochMillis(String dateString) {
		return parse(dateString, ES_TIME_FORMATTER).toInstant().toEpochMilli();
	}

	public static long epochMillis(String dateString, DateTimeFormatter dateTimeFormatter) {
		return parse(dateString, dateTimeFormatter).toInstant().toEpochMilli();
	}

	public static long toEpochMillis(LocalDateTime localDateTime) {
		return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	public static long toEpochMillis(ZonedDateTime zonedDateTime) {
		return zonedDateTime.toInstant().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	public static LocalDateTime epochMillisParse(long timestamp) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
	}

	public static LocalDateTime now() {
		return LocalDateTime.now(ZoneId.systemDefault());
	}

	public static String epochMilliFormat(long epochMillis) {
		return epochMilliFormat(epochMillis, DEFAULT_TS_PATTERN);
	}

	public static String epochMilliFormat(long epochMillis, String pattern) {
		return format(
				ZonedDateTime.ofInstant(
						Instant.ofEpochMilli(epochMillis),
						ZoneId.systemDefault()),
				DateTimeFormatter.ofPattern(pattern == null ? DEFAULT_TS_PATTERN : pattern));
	}

	public static String format(ZonedDateTime date, DateTimeFormatter dateTimeFormatter) {
		return dateTimeFormatter.format(date);
	}

	public static long currentMilliSeconds() {
		return System.currentTimeMillis();
	}


	public static long truncatedToMinutes(long time) {
		return time / 60000 * 60000;
	}

	public static long truncatedToDays(long time) {
		return time - (time % TimeUnit.DAYS.toMillis(1));
	}
}
