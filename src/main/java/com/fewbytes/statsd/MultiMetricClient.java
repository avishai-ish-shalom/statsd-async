package com.fewbytes.statsd;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 11/1/13
 * Time: 11:34 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class MultiMetricClient extends BlockingClient {
    private int BUFFER_CAPACITY = 1024;
    private ByteBuffer bb;

    public MultiMetricClient(String host, int port) throws UnknownHostException, SocketException {
        super(host, port);
        this.bb = ByteBuffer.allocate(BUFFER_CAPACITY);
    }

    protected void appendToBuffer(String payload) {
        if (bb.remaining() < payload.length() + 1) {
            flush();
        }
        if (bb.position() > 0) {
            bb.put((byte) '\n');
        }
        bb.put(payload.getBytes());
    }

    protected void flush() {
        bb.flip();
        this.send(bb);
        bb.limit(bb.capacity());
        bb.rewind();
    }
}
