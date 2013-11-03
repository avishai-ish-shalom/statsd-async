package com.fewbytes.statsd.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * User: avishai
 * Date: 9/25/13
 */


public final class StatsdEvent {
    private String payload;

    public final static EventFactory<StatsdEvent>EVENT_FACTORY = new EventFactory<StatsdEvent>() {
        @Override
        public StatsdEvent newInstance() {
            return new StatsdEvent();  //To change body of implemented methods use File | Settings | File Templates.
        }
    };

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
