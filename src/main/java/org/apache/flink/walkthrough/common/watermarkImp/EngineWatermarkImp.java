package org.apache.flink.walkthrough.common.watermarkImp;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.walkthrough.common.entity.Record;

/**
 * @author zhang lianhui
 * @date 2020/10/29 3:48 下午
 */
public class EngineWatermarkImp implements WatermarkStrategy<Record> {
	@Override
	public WatermarkGenerator<Record> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
		return new Generator();
	}

	public static class Generator implements WatermarkGenerator<Record> {
		private long maxTimestamp;
		private final long DELAY = 3000L;

		@Override
		public void onEvent(Record event, long eventTimestamp, WatermarkOutput output) {
			maxTimestamp = Math.max(maxTimestamp, event.getTimestamp());
		}

		@Override
		public void onPeriodicEmit(WatermarkOutput output) {
			output.emitWatermark(new Watermark(maxTimestamp - DELAY - 1));
		}
	}
}
