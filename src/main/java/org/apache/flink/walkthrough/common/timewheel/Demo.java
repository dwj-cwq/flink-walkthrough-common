package org.apache.flink.walkthrough.common.timewheel;

import io.netty.util.HashedWheelTimer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhang lianhui
 * @date 2020/11/6 5:06 下午
 */
public class Demo {
	public static void main(String[] args) throws InterruptedException {
		test2();

	}

	public static void test1() throws InterruptedException {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//生成了一个时间轮，时间步长为300ms
		HashedWheelTimer hashedWheelTimer = new HashedWheelTimer(300, TimeUnit.MILLISECONDS);
		System.out.println("start:" + LocalDateTime.now().format(formatter));
//提交一个任务在三秒之后允许
		hashedWheelTimer.newTimeout(timeout -> {
			System.out.println("task :" + LocalDateTime.now().format(formatter));
		}, 3, TimeUnit.SECONDS);
		hashedWheelTimer.newTimeout(timeout -> System.out.println(
				"task 2: " + LocalDateTime.now().format(formatter)), 5, TimeUnit.SECONDS);

		Thread.sleep(1000 * 10);

		hashedWheelTimer.newTimeout(timeout -> System.out.println(
				"task 3: " + LocalDateTime.now().format(formatter)), 5, TimeUnit.SECONDS);
	}

	public static void test2() throws InterruptedException {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


		HashedWheelTimer hashedWheelTimer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);
		System.out.println("start:" + LocalDateTime.now().format(formatter));
		hashedWheelTimer.newTimeout(timeout -> {
			Thread.sleep(3000);
			System.out.println("task1:" + LocalDateTime.now().format(formatter));
		}, 3, TimeUnit.SECONDS);
		hashedWheelTimer.newTimeout(timeout ->
				System.out.println("task2:" + LocalDateTime.now().format(
						formatter)), 4, TimeUnit.SECONDS);
	}

	public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
		return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				60L, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(),
				threadFactory);
	}
}
