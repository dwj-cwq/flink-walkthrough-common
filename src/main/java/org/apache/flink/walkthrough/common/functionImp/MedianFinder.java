package org.apache.flink.walkthrough.common.functionImp;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * @author zhang lianhui
 * @date 2020/11/2 3:05 下午
 */
public class MedianFinder {
	public MedianFinder() {

	}

	private Queue<Double> maxHeap = new PriorityQueue<>(Comparator.reverseOrder());
	private Queue<Double> minHeap = new PriorityQueue<>();

	public void addNum(double n) {
		if (n < getMedian()) {
			maxHeap.offer(n);
		} else {
			minHeap.offer(n);
		}
		rebalance();
	}

	public int size() {
		return maxHeap.size() + minHeap.size();
	}

	public void removeNum(double n) {
		if (n < getMedian()) {
			maxHeap.remove(n);
		} else {
			minHeap.remove(n);
		}
		rebalance();
	}

	public void rebalance() {
		// always let minHeap's size >= maxHeap's size
		if (maxHeap.size() > minHeap.size()) {
			minHeap.offer(maxHeap.poll());
		}
		// re-balance process
		if (minHeap.size() - maxHeap.size() > 1) {
			maxHeap.offer(minHeap.poll());
		}
	}

	public double getMedian() {
		if (maxHeap.isEmpty() && minHeap.isEmpty()) {
			return 0;
		}
		return maxHeap.size() == minHeap.size() ?
				(maxHeap.peek() + minHeap.peek()) / 2.0 : minHeap.peek();
	}
}

