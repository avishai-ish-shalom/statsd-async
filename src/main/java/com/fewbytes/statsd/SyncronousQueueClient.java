package com.fewbytes.statsd;

import net.jcip.annotations.ThreadSafe;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * User: avishai
 * Date: 11/1/13
 */


/*
An async StatsD client which uses SynchronousQueue to transfer payload to a single thread executor
 */

@ThreadSafe
public class SyncronousQueueClient extends MultiMetricClient implements Runnable {
    private final Client innerClient;
    private final ExecutorService executor;
    private final SynchronousQueue<String> queue;
    private final boolean lossy;

    public SyncronousQueueClient(String host, int port, boolean lossy) throws SocketException, UnknownHostException {
        super(host, port);
        this.lossy = lossy;
        this.innerClient = new BlockingClient(host, port);
        this.executor = Executors.newSingleThreadExecutor();
        this.queue = new SynchronousQueue<String>(); // fair = false because we don't care about order
        this.executor.execute(this);
    }

    public SyncronousQueueClient(String host, int port) throws SocketException, UnknownHostException {
        this(host, port, true);
    }

    @Override
    protected void send(String payload) {
        if (lossy) {
            queue.offer(payload);
        } else {
            try {
                queue.put(payload);
            } catch (InterruptedException e) {
                logger.error("Interrupted while queueing payload", e);
            }
        }
    }

    @Override
    public void run() {
        String s;
        while (true) {
            try {
                s = queue.take();
                appendToBuffer(s);
            } catch (InterruptedException e) {
                logger.error("Interrupted while appending payload to buffer", e);
            }
        }
    }
}
