package com.fewbytes.statsd;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: avishai
 * Date: 9/23/13
 * Time: 9:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class BlockingClient extends Client {
    private final DatagramSocket socket;
    private final InetAddress HostAddress;
    private final int port;
    private final int BUFFER_SIZE = 262144;

    public BlockingClient(String host, int port) throws SocketException, UnknownHostException {
        socket = new DatagramSocket();
        socket.setSendBufferSize(BUFFER_SIZE);
        this.HostAddress = InetAddress.getByName(host);
        this.port = port;
    }

    public BlockingClient(String host, int port, float scaleFactor) throws SocketException, UnknownHostException {
        this(host, port);
        this.scaleFactor = scaleFactor;
    }

    @Override
    protected void send(String payload) {
        send(ByteBuffer.wrap(payload.getBytes()));
    }

    protected void send(ByteBuffer buff) {
        DatagramPacket packet = new DatagramPacket(buff.array(), buff.arrayOffset(), buff.limit(), this.HostAddress, this.port);
        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
