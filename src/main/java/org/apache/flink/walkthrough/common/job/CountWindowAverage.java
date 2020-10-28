package org.apache.flink.walkthrough.common.job;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author zhang lianhui
 * @date 2020/10/24 11:16 上午
 */
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
	private transient ValueState<Tuple2<Long, Long>> sum;
	private IntCounter counter = new IntCounter();

	@Override
	public void flatMap(
			Tuple2<Long, Long> input,
			Collector<Tuple2<Long, Long>> out) throws Exception {
		final Tuple2<Long, Long> currentValue = sum.value();

		currentValue.f0 += 1;
		currentValue.f1 += input.f1;

		sum.update(currentValue);

		if (currentValue.f0 >= 2) {
			out.collect(new Tuple2<>(input.f0, currentValue.f1 / currentValue.f0));
			sum.clear();
			this.counter.add(1);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
				"average",
				TypeInformation
						.of(new TypeHint<Tuple2<Long, Long>>() {
						}));

		sum = getRuntimeContext().getState(descriptor);
		getRuntimeContext().addAccumulator("demo", this.counter);
	}
}
