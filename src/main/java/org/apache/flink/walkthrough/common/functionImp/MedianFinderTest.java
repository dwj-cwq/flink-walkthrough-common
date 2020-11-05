package org.apache.flink.walkthrough.common.functionImp;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author zhang lianhui
 * @date 2020/11/3 12:02 上午
 */
public class MedianFinderTest {

	@Test
	public void getMedian() {
		final MedianFinder medianFinder = new MedianFinder();
		for (int i = 0; i <= 5; i++) {
			medianFinder.addNum(i * 1.0D);
		}
		System.out.println(medianFinder.getMedian());
	}
}
