package org.apache.flink.walkthrough.common.watermarkImp;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.walkthrough.common.entity.SensorEvent;

/**
 * 在这个onEvent方法里，我们从每个元素里抽取了一个时间字段，但是我们并没有生成水印发射给下游，而是自己保存了在一个变量里，用来更新当前最大时间戳
 * 在onPeriodicEmit方法里，使用最大的日志时间减去我们想要的延迟时间作为水印发射给下游。
 *
 * @author zhang lianhui
 * @date 2020/10/28 5:26 下午
 */
public class MyWatermarkStrategy implements WatermarkStrategy<SensorEvent> {
	@Override
	public WatermarkGenerator<SensorEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
		return new MyWatermarkGenerator();
	}

	public static class MyWatermarkGenerator implements WatermarkGenerator<SensorEvent> {
		private long maxTimestamp;
		private long delay = 3000;

		@Override
		public void onEvent(
				SensorEvent sensorEvent,
				long eventTimestamp,
				WatermarkOutput watermarkOutput) {
			maxTimestamp = Math.max(maxTimestamp, sensorEvent.getTimestamp());
		}

		@Override
		public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
			watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay - 1));
		}
	}
}
