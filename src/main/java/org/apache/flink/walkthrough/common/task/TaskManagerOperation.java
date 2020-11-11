package org.apache.flink.walkthrough.common.task;

/**
 * @author zhang lianhui
 * @date 2020/11/11 11:27 上午
 */
public interface TaskManagerOperation<T, ID> {
	T add(T task);

	boolean delete(ID id);

	T find(ID id);

	boolean cancel(ID id);

	void clear();
}
