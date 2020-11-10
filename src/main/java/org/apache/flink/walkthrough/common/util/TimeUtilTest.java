package org.apache.flink.walkthrough.common.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author zhang lianhui
 * @date 2020/11/10 1:28 下午
 */
public class TimeUtilTest {

	@Test
	public void hiResClockMs() {
		System.out.println(TimeUtil.hiResClockMs());
		System.out.println(TimeUtil.currentMilliSeconds());
	}
}
