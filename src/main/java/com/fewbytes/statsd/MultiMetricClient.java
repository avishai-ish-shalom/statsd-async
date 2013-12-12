package com.fewbytes.statsd;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;

/**
 * User: avishai
 * Date: 11/1/13
 */
public abstract class MultiMetricClient extends BlockingClient {
    private int BUFFER_CAPACITY = 1024;
    private ByteBuffer bb;
    private final Timer flushThread;
    private final int FLUSH_INTERVAL = 2000;

    public MultiMetricClient(String host, int port) throws UnknownHostException, SocketException {
        super(host, port);
        bb = ByteBuffer.allocate(BUFFER_CAPACITY);
        this.flushThread = new Timer("statsd-periodic-flush", true);
        flushThread.schedule(new PeriodicFlush(this), FLUSH_INTERVAL, FLUSH_INTERVAL);
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

    class PeriodicFlush extends TimerTask {
        private final MultiMetricClient statsdClient;

        PeriodicFlush(MultiMetricClient statsdClient1) {
            this.statsdClient = statsdClient1;
        }

        @Override
        public void run() {
            statsdClient.flush();
        }
    }
}
