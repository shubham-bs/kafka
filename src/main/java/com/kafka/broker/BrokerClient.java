package com.kafka.broker;

import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;

import java.io.IOException;
import java.net.Socket;

public final class BrokerClient {

    private BrokerClient() {
    }

    public static ProtocolFrame request(BrokerInfo broker, ProtocolFrame request) throws IOException {

        try (Socket socket = new Socket(broker.getHost(), broker.getPort())) {

            socket.setSoTimeout(5000);

            ProtocolEncoder encoder = new ProtocolEncoder(socket.getOutputStream());

            ProtocolDecoder decoder = new ProtocolDecoder(socket.getInputStream());

            encoder.writeFrame(request);

            return decoder.readFrame();
        }
    }
}