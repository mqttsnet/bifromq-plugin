package bifromq.plugin.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Queue;

@Slf4j
public class TaskQueue {

    private Queue<Runnable> tasks = new LinkedList<>();
    private final Object lock = new Object();

    // 添加任务到队列
    public void addTask(Runnable task) {
        synchronized (lock) {
            tasks.add(task);
            lock.notify();
        }
    }

    // 在一个线程中循环执行队列中的任务
    public void startExecuting() {
        new Thread(() -> {
            while (true) {
                log.info("executeTask start");
                executeTask();
                log.info("executeTask end");
            }
        }).start();
    }

    // 从队列中取出一个任务并执行
    private void executeTask() {
        Runnable task = null;

        synchronized (lock) {
            while (tasks.isEmpty()) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // Restore the interrupted status
                    return;
                }
            }
            task = tasks.poll();
        }

        if (task != null) {
            task.run();
        }
    }
}
