package org.apache.flink.walkthrough.common.task;

/**
 * @author zhang lianhui
 * @date 2020/11/12 3:55 下午
 */
public class CronCacheFactory {
	private static volatile CronCache cronCache;

	private CronCacheFactory() {
	}

	public static CronCache getCronCache() {
		if (cronCache == null) {
			synchronized (CronCacheFactory.class) {
				cronCache = new CronCache();
			}
		}
		return cronCache;
	}
}
