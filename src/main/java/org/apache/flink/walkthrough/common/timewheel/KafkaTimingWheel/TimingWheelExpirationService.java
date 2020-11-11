package org.apache.flink.walkthrough.common.timewheel.KafkaTimingWheel;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhang lianhui
 * @date 2020/11/10 4:42 下午
 */
@Slf4j
public class TimingWheelExpirationService {
	private final Timer timer;
	private ExpiredOperationReaper reaper;

	public TimingWheelExpirationService(Timer timer) {
		this.timer = timer;
	}

	public void start() {
		log.info("expired operation reaper thread started");
		reaper = new ExpiredOperationReaper(timer);
		reaper.setDaemon(true);
		reaper.start();
	}

	public void shutdown() {
		reaper.shutdown();
	}

	/**
	 * 过期操作收割机/后台过期操作收割线程
	 */
	private static class ExpiredOperationReaper extends ShutdownAbleThread {
		private static final String NAME = "expiration-reaper";
		private static final long WORK_TIMEOUT_MS = 200L;

		private final Timer timer;

		public ExpiredOperationReaper(Timer timer) {
			super(NAME);
			this.timer = timer;
		}

		@Override
		public void doWork() {
			timer.advanceClock(WORK_TIMEOUT_MS);
		}
	}
}
