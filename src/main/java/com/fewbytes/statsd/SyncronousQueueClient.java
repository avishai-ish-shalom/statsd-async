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
    Client innerClient;
    ExecutorService executor;
    SynchronousQueue<String> queue;

    public SyncronousQueueClient(String host, int port) throws SocketException, UnknownHostException {
        super(host, port);
        this.innerClient = new BlockingClient(host, port);
        this.executor = Executors.newSingleThreadExecutor();
        this.queue = new SynchronousQueue<String>(); // fair = false because we don't care about order
        this.executor.execute(this);
    }

    @Override
    protected void send(String payload) {
        queue.offer(payload);
    }

    @Override
    public void run() {
        String s;
        while (true) {
            try {
                s = queue.take();
                appendToBuffer(s);
            } catch (InterruptedException e) {
                logger.error("couldn't append payload to buffer", e);
            }
        }
    }
}
