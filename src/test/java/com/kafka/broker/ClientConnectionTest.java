package com.kafka.broker;

import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ClientConnectionTest {

    @Test
    void shouldHandlePingRequest() throws Exception {

        KafkaBroker broker = new KafkaBroker(0, 2);

        Thread brokerThread = new Thread(() -> {
                    try {
                        broker.start();
                    } catch (Exception e) {
                        if (broker.getPort() != -1) {
                            fail(e);
                        }
                    }
                });

        brokerThread.start();

        try {
            waitForBroker(broker);
            try (Socket socket = new Socket("localhost", broker.getPort())) {
                ProtocolEncoder encoder = new ProtocolEncoder(socket.getOutputStream());
                ProtocolDecoder decoder = new ProtocolDecoder(socket.getInputStream());
                ProtocolFrame request = new ProtocolFrame(
                                ProtocolFrame.CURRENT_VERSION,
                                RequestType.PING,
                                42,
                                "hello".getBytes(
                                        StandardCharsets.UTF_8
                                )
                        );

                encoder.writeFrame(request);

                ProtocolFrame response = decoder.readFrame();

                assertEquals(RequestType.PING, response.getRequestType());

                assertEquals(42, response.getCorrelationId());

                assertArrayEquals("PONG".getBytes(StandardCharsets.UTF_8), response.getPayload());
            }
        } finally {
            broker.shutdown();
            brokerThread.join(2000);
        }
    }

    private void waitForBroker(KafkaBroker broker) throws InterruptedException {

        long deadline = System.currentTimeMillis() + 2000;

        while (broker.getPort() == -1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertNotEquals(-1, broker.getPort(), "Broker failed to start");
    }
}