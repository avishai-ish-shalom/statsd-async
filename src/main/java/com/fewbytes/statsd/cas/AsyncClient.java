package com.fewbytes.statsd.cas;

import com.fewbytes.statsd.BlockingClient;
import com.fewbytes.statsd.MultiMetricClient;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 10/16/13
 * Time: 6:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class AsyncClient extends MultiMetricClient {
    private AtomicBoolean ready;
    private int BUFFER_CAPACITY = 1024;
    private ByteBuffer bb;

    public AsyncClient(String host, int port) throws UnknownHostException, SocketException {
        super(host, port);
        this.ready = new AtomicBoolean(true);
        this.bb = ByteBuffer.allocate(BUFFER_CAPACITY);
    }

    @Override
    protected void send(String payload) {
        if (this.ready.compareAndSet(true, false)) {
            appendToBuffer(payload);
            this.ready.set(true);
        } // else we drop the message on the floor to avoid blocking
    }

}
