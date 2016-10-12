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
    double scaleFactor = 1;
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final Random RNG = new Random();

    public void incr(String name, int count, double scaleFactor) {
        if (scaleFactor == 1 || RNG.nextFloat() < scaleFactor) {
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

    public void decr(String name, int count, double scaleFactor) {
        incr(name, -count, scaleFactor);
    }

    public void timer(String name, double value, double scaleFactor) {
        if (scaleFactor == 1 || RNG.nextFloat() < scaleFactor) {
            StringBuilder sb = new StringBuilder(name);
            sb.append(":")
                    .append(value)
                    .append("|ms");
            if (scaleFactor != 1) {
                sb.append("@")
                        .append(scaleFactor);
            }
            send(sb.toString());
        }
    }

    public boolean gauge(String name, double value) {
        return gauge(name, value, this.scaleFactor);
    }

    public boolean gauge(String name, double value, double scaleFactor) {
        if (RNG.nextFloat() < scaleFactor) {
             send(
                new StringBuilder(name)
                        .append(":")
                        .append(value)
                        .append("|g")
                        .toString()
            );
            return true;
        }
        return false;
    }

    public void timer(String name, double value) {
        timer(name, value, this.scaleFactor);
    }

    abstract protected void send(String payload);
}
