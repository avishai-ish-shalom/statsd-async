package com.fewbytes.statsd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 9/23/13
 * Time: 8:45 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Client implements IClient {
    float scaleFactor = 1;
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final Random RNG = new Random();

    public void incr(String name, int count, float scaleFactor) {
        if (RNG.nextFloat() < scaleFactor) {
            send(
                    new StringBuilder(name)
                            .append(":")
                            .append(count)
                            .append("|c@")
                            .append(scaleFactor)
                            .toString()
            );
        }
    }

    public void incr(String name, int count) {
        incr(name, count, this.scaleFactor);
    }

    public void incr(String name) {
        incr(name, 1, this.scaleFactor);
    }

    public void decr(String name, int count) {
        incr(name, -count, this.scaleFactor);
    }

    public void decr(String name) {
        incr(name, -1, this.scaleFactor);
    }

    public void decr(String name, int count, float scaleFactor) {
        incr(name, -count, scaleFactor);
    }

    public void timer(String name, float value, float scaleFactor) {
        if (RNG.nextFloat() < scaleFactor) {
            send(
                new StringBuilder(name)
                        .append(":")
                        .append(value)
                        .append("|ms")
                        .toString()
            );
        }
    }

    public void gauge(String name, float value) {
        gauge(name, value, this.scaleFactor);
    }

    public void gauge(String name, float value, float scaleFactor) {
        if (RNG.nextFloat() < scaleFactor) {
            send(
                new StringBuilder(name)
                        .append(":")
                        .append(value)
                        .append("|g")
                        .toString()
            );
        }
    }

    public void timer(String name, float value) {
        timer(name, value, this.scaleFactor);
    }

    abstract protected void send(String payload);
}
