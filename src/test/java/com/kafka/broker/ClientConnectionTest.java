package com.kafka.broker;

import com.kafka.protocol.ProduceRequest;
import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ClientConnectionTest {

    @TempDir
    Path tempDirectory;

    @Test
    void shouldHandlePingRequest() throws Exception {

        KafkaBroker broker = new KafkaBroker(0, 2, tempDirectory);

        Thread brokerThread = startBroker(broker);

        try {
            waitForBroker(broker);
            try (Socket socket = new Socket("localhost", broker.getPort())) {

                ProtocolEncoder encoder = new ProtocolEncoder(socket.getOutputStream());
                ProtocolDecoder decoder = new ProtocolDecoder(socket.getInputStream());

                ProtocolFrame request = new ProtocolFrame(
                                ProtocolFrame.CURRENT_VERSION,
                                RequestType.PING,
                                42,
                                "hello".getBytes(StandardCharsets.UTF_8));

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

    @Test
    void shouldProduceMessageThroughTcp() throws Exception {
        KafkaBroker broker = new KafkaBroker(0, 2, tempDirectory);

        Thread brokerThread = startBroker(broker);

        try {
            waitForBroker(broker);
            try (Socket socket = new Socket("localhost", broker.getPort())){

                ProtocolEncoder encoder = new ProtocolEncoder(socket.getOutputStream());

                ProtocolDecoder decoder = new ProtocolDecoder(socket.getInputStream());

                ProduceRequest produceRequest = new ProduceRequest("orders", 0,
                                "hello".getBytes(StandardCharsets.UTF_8));

                ProtocolFrame request = new ProtocolFrame(
                                ProtocolFrame.CURRENT_VERSION,
                                RequestType.PRODUCE,
                                100,
                                produceRequest.encode());

                encoder.writeFrame(request);

                ProtocolFrame response = decoder.readFrame();

                assertEquals(RequestType.PRODUCE, response.getRequestType());

                assertEquals(100, response.getCorrelationId());

                long offset = ByteBuffer.wrap(response.getPayload()).getLong();

                assertEquals(0, offset);

                assertEquals(1, broker
                                .getTopicManager()
                                .getOrCreateTopic(
                                        "orders"
                                )
                                .getPartition(0)
                                .size());
            }

        } finally {
            broker.shutdown();
            brokerThread.join(2000);
        }
    }

    private Thread startBroker(KafkaBroker broker) {

        Thread thread = new Thread(() -> {
                    try {
                        broker.start();
                    } catch (Exception e) {
                        if (broker.getPort() != -1) throw new RuntimeException(e);
                    }
                });

        thread.start();

        return thread;
    }

    private void waitForBroker(KafkaBroker broker) throws InterruptedException {

        long deadline = System.currentTimeMillis() + 2000;

        while (broker.getPort() == -1 && System.currentTimeMillis() < deadline){
            Thread.sleep(10);
        }

        assertNotEquals(-1, broker.getPort(), "Broker failed to start");
    }
}