package com.kafka.broker;

import com.kafka.protocol.FetchRequest;
import com.kafka.protocol.ProduceRequest;
import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ClientConnectionTest {

    @Test
    void shouldProduceAndFetchMessage(@TempDir Path tempDirectory) throws Exception {

        KafkaBroker broker = new KafkaBroker(
                        0,
                        2,
                        tempDirectory);

        Thread brokerThread = new Thread(() -> {
                    try {
                        broker.start();
                    } catch (Exception ignored) {
                    }
                });

        brokerThread.start();

        waitForBrokerPort(broker);

        try (Socket socket = new Socket("localhost", broker.getPort());

                InputStream input = socket.getInputStream();

                OutputStream output = socket.getOutputStream()
        ) {

            ProtocolEncoder encoder = new ProtocolEncoder(output);

            ProtocolDecoder decoder = new ProtocolDecoder(input);

            byte[] message = "hello kafka".getBytes(StandardCharsets.UTF_8);

            ProduceRequest produceRequest = new ProduceRequest(
                            "orders",
                            0,
                            message);

            ProtocolFrame produceFrame = new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.PRODUCE,
                            100L,
                            produceRequest.encode());

            encoder.writeFrame(produceFrame);

            ProtocolFrame produceResponse = decoder.readFrame();

            assertEquals(RequestType.PRODUCE, produceResponse.getRequestType());

            assertEquals(100L, produceResponse.getCorrelationId());

            long producedOffset = ByteBuffer.wrap(produceResponse.getPayload()).getLong();

            assertEquals(0L, producedOffset);

            FetchRequest fetchRequest = new FetchRequest(
                            "orders",
                            0,
                            0L);

            ProtocolFrame fetchFrame = new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.FETCH,
                            200L,
                            fetchRequest.encode());

            encoder.writeFrame(fetchFrame);

            ProtocolFrame fetchResponse = decoder.readFrame();

            assertEquals(RequestType.FETCH, fetchResponse.getRequestType());

            assertEquals(200L, fetchResponse.getCorrelationId());

            ByteBuffer responseBuffer = ByteBuffer.wrap(fetchResponse.getPayload());

            long fetchedOffset = responseBuffer.getLong();

            int messageLength = responseBuffer.getInt();

            byte[] fetchedMessage = new byte[messageLength];

            responseBuffer.get(fetchedMessage);

            assertEquals(0L, fetchedOffset);

            assertArrayEquals(message, fetchedMessage);
        } finally {
            broker.shutdown();
            brokerThread.join(2000);
        }
    }

    @Test
    void shouldReturnEndOfPartition(@TempDir Path tempDirectory) throws Exception {

        KafkaBroker broker = new KafkaBroker(
                        0,
                        2,
                        tempDirectory);

        Thread brokerThread = new Thread(() -> {
                    try {
                        broker.start();
                    } catch (Exception ignored) {
                    }
                });

        brokerThread.start();

        waitForBrokerPort(broker);

        try (
                Socket socket = new Socket(
                                "localhost",
                                broker.getPort());

                InputStream input = socket.getInputStream();

                OutputStream output = socket.getOutputStream()
        ) {

            ProtocolEncoder encoder = new ProtocolEncoder(output);

            ProtocolDecoder decoder = new ProtocolDecoder(input);

            FetchRequest fetchRequest = new FetchRequest(
                            "orders",
                            0,
                            0L);

            ProtocolFrame request = new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.FETCH,
                            300L,
                            fetchRequest.encode());

            encoder.writeFrame(request);

            ProtocolFrame response = decoder.readFrame();

            assertEquals(RequestType.FETCH, response.getRequestType());

            long offset = ByteBuffer.wrap(response.getPayload()).getLong();

            assertEquals(-1L, offset);

        } finally {
            broker.shutdown();
            brokerThread.join(2000);
        }
    }

    private void waitForBrokerPort(KafkaBroker broker) throws Exception {

        long deadline = System.currentTimeMillis() + 2000;

        while (broker.getPort() <= 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        if (broker.getPort() <= 0) {
            throw new IllegalStateException("Broker did not start");
        }
    }
}