package test;

import com.google.common.base.Objects;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel.TimerTask;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhang lianhui
 * @date 2020/11/10 4:58 下午
 */
@Slf4j
public class TestTask extends TimerTask {
	private final int id;
	private final CountDownLatch latch;
	private final List<Integer> output;
	private final AtomicBoolean completed = new AtomicBoolean(false);

	public long getId() {
		return id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TestTask testTask = (TestTask) o;
		return delayMs == testTask.delayMs &&
				id == testTask.id &&
				Objects.equal(completed, testTask.completed);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(delayMs, id, completed);
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	public List<Integer> getOutput() {
		return output;
	}

	@Override
	public long getDelayMs() {
		return delayMs;
	}

	public TestTask(long delayMs, int id, CountDownLatch latch, List<Integer> output) {
		this.delayMs = delayMs;
		this.id = id;
		this.latch = latch;
		this.output = output;
	}


	@Override
	public void run() {
		if (completed.compareAndSet(false, true)) {
			output.add(id);
			log.info("runing test task: {}", this);
			latch.countDown();
		}
	}

	@Override
	public String toString() {
		return "TestTask{" +
				"delayMs=" + delayMs +
				", id=" + id +
				", completed=" + completed +
				'}';
	}
}
