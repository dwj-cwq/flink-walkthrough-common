package test;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.shaded.curator4.com.google.common.collect.Sets;

import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.SystemTimer;
import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.Timer;
import org.apache.flink.walkthrough.common.util.TimeUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author zhang lianhui
 * @date 2020/11/9 8:02 下午
 */
@Slf4j
public class TimerTest {
	private Timer timer;

	@Before
	public void setUp() {
		timer = new SystemTimer("test", 1, 3, TimeUtil.hiResClockMs());
	}

	@After
	public void teardown() {
		timer.shutdown();
	}

	@Test
	public void testAlreadyExpiredTask() {
		List<Integer> output = new CopyOnWriteArrayList<>();
		List<CountDownLatch> latches = new CopyOnWriteArrayList<>();
		for (int i = -5; i < 0; i++) {
			final CountDownLatch latch = new CountDownLatch(1);
			final TestTask testTask = new TestTask(i, i, latch, output);
			System.out.println("created test task: " + testTask);
			timer.add(testTask);
			latches.add(latch);
		}

		timer.advanceClock(0);
		latches.forEach(latch -> {
			try {
				assertEquals("already expired tasks should run immediately", true, latch.await(
						3,
						TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

		assertEquals(
				"output of already expired tasks",
				Sets.newHashSet(-5, -4, -3, -2, -1),
				Sets.newHashSet(output));


	}

	@Test
	public void DelayedOperation() {
		List<Integer> output = new CopyOnWriteArrayList<>();
		List<TestTask> tasks = new CopyOnWriteArrayList<>();
		List<Integer> ids = new ArrayList<>();
		List<CountDownLatch> latches = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			CountDownLatch latch = new CountDownLatch(1);
			tasks.add(new TestTask(i, i, latch, output));
			ids.add(i);
			latches.add(latch);
		}

		for (int i = 10; i < 100; i++) {
			CountDownLatch latch = new CountDownLatch(2);
			tasks.add(new TestTask(i, i, latch, output));
			tasks.add(new TestTask(i, i, latch, output));
			ids.add(i);
			ids.add(i);
			latches.add(latch);
		}

		for (int i = 100; i < 500; i++) {
			CountDownLatch latch = new CountDownLatch(1);
			tasks.add(new TestTask(i, i, latch, output));
			ids.add(i);
			latches.add(latch);
		}

		tasks.forEach(t -> timer.add(t));

		while (timer.advanceClock(2000)) {
		}
		latches.forEach(latch -> {
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		ids.sort(Integer::compare);
		assertEquals("output should match", ids, new ArrayList<>(output));

	}

	@Test
	public void getSize() {
		System.out.println("demo");
	}

	@Test
	public void shutdown() {
	}

	@Test
	public void testDelay() {
		List<Integer> output = new CopyOnWriteArrayList<>();
		List<TestTask> tasks = new ArrayList<>();
		List<Integer> ids = new ArrayList<>();
		List<CountDownLatch> latches = new ArrayList<>();
		for (int i = 1; i < 2; i++) {
			CountDownLatch latch = new CountDownLatch(1);
			tasks.add(new TestTask(i * 1000_0, i, latch, output));
			ids.add(i);
			latches.add(latch);
		}

		for (TestTask task : tasks) {
			timer.add(task);
		}

		while (timer.advanceClock(15000)) {
		}

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
