package com.fewbytes.statsd.disruptor;

import com.fewbytes.statsd.Client;
import com.fewbytes.statsd.MultiMetricClient;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: avishai
 */

/*
  An async StatsD client which uses LMAX disruptor to hand off payload to worker thread.
  This client is lossy and will drop events silently if it cannot obtain a slot from the ring buffer sequencer
 */

@ThreadSafe
public class AsyncClient extends Client implements EventTranslatorOneArg<StatsdEvent, String> {
    private final Disruptor<StatsdEvent> disruptor;
    private final Executor executor;
    private final RingBuffer<StatsdEvent> ringBuffer;
    private final int RING_BUFFER_SIZE = 262144;
    private final boolean lossy;
    private AtomicLong lost = new AtomicLong(0);
    private AtomicLong sent = new AtomicLong(0);

    public AsyncClient(String host, int port, boolean lossy) throws IOException {
        this.lossy = lossy;
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactory () {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        });
        this.disruptor = new  Disruptor<StatsdEvent>(StatsdEvent.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                this.executor,
                ProducerType.MULTI,
                new SleepingWaitStrategy());
        this.disruptor.handleEventsWith(new StatsDEventHandler(host, port));
        this.ringBuffer = this.disruptor.start();
    }

    public AsyncClient(String host, int port) throws IOException {
        this(host, port, true);
    }

    public void translateTo(StatsdEvent event, long sequence, String data) {
        event.setPayload(data);
    }

    @Override
    protected void send(final String data) {
        if (lossy) {
            if (!this.ringBuffer.tryPublishEvent(this, data)) {
                // if we can't publish, silently drop the payload
                lost.incrementAndGet();
            }
        } else {
            this.ringBuffer.publishEvent(this, data);
        }
        sent.incrementAndGet();
    }

    public double getLossRatio() {
        return lost.doubleValue() / sent.get();
    }

    private class StatsDEventHandler extends MultiMetricClient implements EventHandler<StatsdEvent> {
        StatsDEventHandler(String host, int port) throws IOException {
            super(host, port);
        }

        public void onEvent(StatsdEvent statsdEvent, long l, boolean b) throws Exception {
            send(statsdEvent.getPayload());
        }
    }
}
