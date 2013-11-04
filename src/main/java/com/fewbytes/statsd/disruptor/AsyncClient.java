package com.fewbytes.statsd.disruptor;

import com.fewbytes.statsd.BlockingClient;
import com.fewbytes.statsd.Client;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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

    public AsyncClient(String host, int port, boolean lossy) throws IOException {
        this.lossy = lossy;
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactory () {
            @Override
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
                new YieldingWaitStrategy());
        this.disruptor.handleEventsWith(new StatsDEventHandler(host, port));
        this.ringBuffer = this.disruptor.start();
    }

    public AsyncClient(String host, int port) throws IOException {
        this(host, port, true);
    }

    @Override
    public void translateTo(StatsdEvent event, long sequence, String data) {
        event.setPayload(data);
    }

    @Override
    protected void send(final String data) {
        if (lossy) {
            this.ringBuffer.tryPublishEvent(this, data); // if we can't publish, silently drop the payload
        } else {
            this.ringBuffer.publishEvent(this, data);
        }
    }

    class StatsDEventHandler extends BlockingClient implements EventHandler<StatsdEvent> {
        public StatsDEventHandler(String host, int port) throws IOException {
            super(host, port);
        }

        @Override
        public void onEvent(StatsdEvent statsdEvent, long l, boolean b) throws Exception {
            send(statsdEvent.getPayload());
        }
    }
}
