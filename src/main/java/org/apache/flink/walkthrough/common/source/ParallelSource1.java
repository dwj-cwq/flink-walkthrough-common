package org.apache.flink.walkthrough.common.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author zhang lianhui
 * @date 2020/11/12 6:57 下午
 */
public class ParallelSource1 implements ParallelSourceFunction<Long> {
	private volatile boolean isCancel = false;
	private long count = 1L;

	@Override
	public void run(SourceContext<Long> out) throws Exception {
		while (!isCancel) {
			System.out.printf("thread: %s, count: %s\n", Thread.currentThread().getName(), count);
			out.collect(count++);
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isCancel = true;
	}
}
