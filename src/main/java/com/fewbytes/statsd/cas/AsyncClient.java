package com.fewbytes.statsd.cas;

import com.fewbytes.statsd.MultiMetricClient;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: avishai
 * Date: 10/16/13
 */

/*
An async StatsD client which uses AtomicBoolean to guard the internal buffer.
 */

public class AsyncClient extends MultiMetricClient {
    private AtomicBoolean ready;
    private AtomicLong lost = new AtomicLong(0);
    private AtomicLong sent = new AtomicLong(0);

    public AsyncClient(String host, int port) throws UnknownHostException, SocketException {
        super(host, port);
        this.ready = new AtomicBoolean(true);
    }

    @Override
    protected void send(String payload) {
        if (this.ready.compareAndSet(true, false)) {
            appendToBuffer(payload);
            this.ready.set(true);
        } else {
            // else we drop the message on the floor to avoid blocking
            lost.incrementAndGet();
        }
        sent.incrementAndGet();
    }

    public double getLossRatio() {
        return lost.doubleValue() / sent.get();
    }

}
