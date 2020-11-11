package test;

import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.SystemTimer;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.Timer;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimingWheelExpirationService;
import org.apache.flink.walkthrough.common.util.TimeUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * @author zhang lianhui
 * @date 2020/11/10 4:56 下午
 */
public class TimingWheelExpirationServiceTest {
	private Timer timer;

	@Before
	public void setUp() {
		timer = new SystemTimer("test", 1, 64, TimeUtil.hiResClockMs());
	}

	@After
	public void teardown() {
		timer.shutdown();
	}

	@Test
	public void testDelay() {
		List<Integer> output = new CopyOnWriteArrayList<>();
		List<TestTask> tasks = new ArrayList<>();
		List<Integer> ids = new ArrayList<>();
		List<CountDownLatch> latches = new ArrayList<>();
		for (int i = 1; i < 120; i++) {
			CountDownLatch latch = new CountDownLatch(1);
			tasks.add(new TestTask(i * 1000, i, latch, output));
			ids.add(i);
			latches.add(latch);
		}

		for (TestTask task : tasks) {
			timer.add(task);
		}

		TimingWheelExpirationService expirationService = new TimingWheelExpirationService(timer);
		expirationService.start();

		latches.forEach(latch -> {
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		ids.sort(Integer::compare);
	}
}
