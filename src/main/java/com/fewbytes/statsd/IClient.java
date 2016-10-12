package com.fewbytes.statsd;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 9/23/13
 * Time: 8:46 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IClient {
    // Increment a counter by count
    public void incr(String name, int count, double scaleFactor);
    public void incr(String name, int count);
    public void incr(String name);
    // Decrement a counter by count
    public void decr(String name, int count, double scaleFactor);
    public void decr(String name, int count);
    public void decr(String name);

    // Set the value of a gauge
    public boolean gauge(String name, double value, double scaleFactor);
    public boolean gauge(String name, double value);

    // Set the value of a timer
    public void timer(String name, double value, double scaleFactor);

    public void timer(String name, double value);
}
