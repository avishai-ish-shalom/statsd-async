package com.fewbytes.statsd.cas;

import com.fewbytes.statsd.MultiMetricClient;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: avishai
 * Date: 10/16/13
 */

/*
An async StatsD client which uses AtomicBoolean to guard the internal buffer.
 */

public class AsyncClient extends MultiMetricClient {
    private AtomicBoolean ready;

    public AsyncClient(String host, int port) throws UnknownHostException, SocketException {
        super(host, port);
        this.ready = new AtomicBoolean(true);
    }

    @Override
    protected void send(String payload) {
        if (this.ready.compareAndSet(true, false)) {
            appendToBuffer(payload);
            this.ready.set(true);
        } // else we drop the message on the floor to avoid blocking
    }

}
