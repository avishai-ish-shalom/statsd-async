package com.fewbytes.statsd;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 11/1/13
 * Time: 11:23 PM
 * To change this template use File | Settings | File Templates.
 */
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
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
}
