package org.apache.flink.walkthrough.common.task;

/**
 * 单例 taskManager
 *
 * @author zhang lianhui
 * @date 2020/11/11 11:43 上午
 */
public class TaskManagerFactory {
	private static volatile TaskManager taskManager;

	private TaskManagerFactory() {
	}

	public static TaskManager getTaskManager() {
		if (taskManager == null) {
			synchronized (TaskManagerFactory.class) {
				taskManager = new TaskManager();
				taskManager.init();
			}
		}
		return taskManager;
	}
}
