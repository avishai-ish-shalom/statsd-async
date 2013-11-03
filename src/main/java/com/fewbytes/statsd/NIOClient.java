package com.fewbytes.statsd;

import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * User: avishai
 */

@ThreadSafe
public class NIOClient extends Client {
    private final InetSocketAddress address;
    private final DatagramChannel channel;
    private final int BUFFER_SIZE = 262144;

    public NIOClient(String host, int port) throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
        this.channel.socket().setSendBufferSize(BUFFER_SIZE);
        this.address = new InetSocketAddress(host, port);
    }

    public NIOClient(String host, int port, float scaleFactor) throws IOException {
        this(host, port);
        this.scaleFactor = scaleFactor;
    }

    @Override
    protected void send(String payload) {
        ByteBuffer bb;
        try {
            bb = ByteBuffer.wrap(payload.getBytes("utf-8"));
            try {
                channel.send(bb, this.address);
            } catch (IOException e) {
                logger.error("Failed sending metric", e);
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("Failed to get bytes from string payload", e);
        }
    }
}
