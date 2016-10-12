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
    private static final int BUFFER_CAPACITY = 64*1024;
    private ByteBuffer bb;
    private final Timer flushThread;
    private final int FLUSH_INTERVAL = 2000;

    public MultiMetricClient(String host, int port) throws  UnknownHostException, SocketException {
        this(host, port, BUFFER_CAPACITY);
    }

    public MultiMetricClient(String host, int port, int bufferSize) throws UnknownHostException, SocketException {
        super(host, port);
        bb = ByteBuffer.allocate(bufferSize);
        this.flushThread = new Timer();
        flushThread.schedule(new PeriodicFlush(this), FLUSH_INTERVAL, FLUSH_INTERVAL);
    }

    protected void appendToBuffer(String payload) {
        if (payload.isEmpty()) {
            flush();
            return;
        } else if (bb.remaining() < payload.length() + 1) {
            flush();
        }
        if (bb.position() > 0) {
            bb.put((byte) '\n');
        }
        bb.put(payload.getBytes());
    }

    protected void flush() {
        if (bb.position() > 0) {
            bb.flip();
            this.send(bb);
            bb.limit(bb.capacity());
            bb.rewind();
        }
    }

    public void signalFlush() {
        this.send(""); // we use empty string to signal a flush
    }

    protected void finalize() {
        flush();
    }

    class PeriodicFlush extends TimerTask {
        private final MultiMetricClient statsdClient;

        PeriodicFlush(MultiMetricClient statsdClient1) {
            this.statsdClient = statsdClient1;
        }

        @Override
        public void run() {
            statsdClient.signalFlush();
        }
    }
}
