package org.apache.flink.walkthrough.common.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.walkthrough.common.entity.Rule;

import java.io.Serializable;

/**
 * @author zhang lianhui
 * @date 2020/11/11 3:26 下午
 */
@Public
public class MonitorSource2 extends FromIteratorFunction<Rule> implements Serializable {
	private static final long serialVersionUID = -6560586535485671809L;

	public MonitorSource2() {
		super(new MonitorSourceIterator2().init());
	}

	@Override
	public void cancel() {
		super.cancel();
	}
}
