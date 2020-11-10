package org.apache.flink.walkthrough.common.timewheel.simpleTimeWheel;

/**
 * @author zhang lianhui
 * @date 2020/11/5 4:42 下午
 */
public abstract class SimpleTask implements Runnable {
	private int cycleNum;
	private int key;
	private int index;

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getCycleNum() {
		return cycleNum;
	}

	public void setCycleNum(int cycleNum) {
		this.cycleNum = cycleNum;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}
}
