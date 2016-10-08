package com.fewbytes.statsd;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by avishai on 31/07/14.
 */
public class ConcurrentLinkedQueueClient extends MultiMetricClient implements Runnable {
    private ConcurrentLinkedQueue<String> queue;
    private ExecutorService executor;
    private static final int DEFAULT_THREAD_PRIORITY = 7;

    public ConcurrentLinkedQueueClient(String host, int port) throws UnknownHostException, SocketException {
        this(host, port, DEFAULT_THREAD_PRIORITY);
    }

    public ConcurrentLinkedQueueClient(String host, int port, final int threadPriority) throws UnknownHostException, SocketException {
        super(host, port);
        queue = new ConcurrentLinkedQueue<String>();
        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("statsd-flusher");
                thread.setDaemon(true);
                thread.setPriority(threadPriority);
                return thread;
            }
        });
        executor.submit(this);
    }

    @Override
    protected void send(String payload) {
        if (!queue.offer(payload)) {
            logger.info("Dropped metric on the floor");
        }
    }

    public void run() {
        String s;
        while (true) {
            s = queue.poll();
            if (s != null) {
                appendToBuffer(s);
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
