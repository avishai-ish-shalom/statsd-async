package com.fewbytes.statsd.disruptor;

import com.fewbytes.statsd.BlockingClient;
import com.fewbytes.statsd.Client;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 9/25/13
 * Time: 9:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class AsyncClient extends Client implements EventTranslatorOneArg<StatsdEvent, String> {
    private final Disruptor<StatsdEvent> disruptor;
    private final Executor executor;
    private final RingBuffer<StatsdEvent> ringBuffer;

    public AsyncClient(String host, int port) throws IOException {
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactory () {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        });
        this.disruptor = new  Disruptor<StatsdEvent>(StatsdEvent.EVENT_FACTORY,
                262144,
                this.executor,
                ProducerType.MULTI,
                new YieldingWaitStrategy());
        this.disruptor.handleEventsWith(new StatsDEventHandler(host, port));
        this.ringBuffer = this.disruptor.start();
    }

    @Override
    public void translateTo(StatsdEvent event, long sequence, String data) {
        event.setPayload(data);
    }

    @Override
    protected void send(final String data) {
        this.ringBuffer.tryPublishEvent(this, data);
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
