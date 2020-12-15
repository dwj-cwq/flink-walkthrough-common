package test;

import org.apache.flink.walkthrough.common.entity.Rule;
import org.apache.flink.walkthrough.common.source.MonitorSourceIterator;
import org.apache.flink.walkthrough.common.source.MonitorSourceIterator2;

/**
 * @author zhang lianhui
 * @date 2020/11/12 12:50 下午
 */
public class DelayTest {
	private static final long LIMIT = Integer.MAX_VALUE;

	public static void twTest() {
		final MonitorSourceIterator2 iterator2 = new MonitorSourceIterator2().init();
		long sum = 0;
		while (iterator2.hasNext() && sum < LIMIT) {
			final Rule rule = iterator2.next();
			System.out.println(rule);
			sum++;
		}
	}

	public static void quartzTest() {
		final MonitorSourceIterator iterator = new MonitorSourceIterator().init();
		long sum = 0;
		while (iterator.hasNext() && sum < LIMIT) {
			final Rule rule = iterator.next();
			System.out.println(rule);
			sum++;
		}
	}

	public static void main(String[] args) {
		twTest();
		// quartzTest();
	}
}
