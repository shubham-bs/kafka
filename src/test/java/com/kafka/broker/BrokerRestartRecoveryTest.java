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

class BrokerRestartRecoveryTest {

    @Test
    void shouldRecoverProducedMessageAfterBrokerRestart(
            @TempDir Path dataDirectory) throws Exception {

        KafkaBroker firstBroker =
                startBroker(dataDirectory);

        long producedOffset;

        try (Socket socket =
                     new Socket("127.0.0.1", firstBroker.getPort());
             InputStream input = socket.getInputStream();
             OutputStream output = socket.getOutputStream()) {

            ProtocolEncoder encoder = new ProtocolEncoder(output);
            ProtocolDecoder decoder = new ProtocolDecoder(input);

            byte[] message =
                    "survives restart".getBytes(StandardCharsets.UTF_8);

            ProduceRequest produce =
                    new ProduceRequest("recovery", 0, message);

            encoder.writeFrame(new ProtocolFrame(
                    ProtocolFrame.CURRENT_VERSION,
                    RequestType.PRODUCE,
                    1L,
                    produce.encode()
            ));

            ProtocolFrame response = decoder.readFrame();

            assertEquals(RequestType.PRODUCE, response.getRequestType());

            producedOffset =
                    ByteBuffer.wrap(response.getPayload()).getLong();

            assertEquals(0L, producedOffset);
        } finally {
            firstBroker.shutdown();
        }

        KafkaBroker secondBroker =
                startBroker(dataDirectory);

        try (Socket socket =
                     new Socket("127.0.0.1", secondBroker.getPort());
             InputStream input = socket.getInputStream();
             OutputStream output = socket.getOutputStream()) {

            ProtocolEncoder encoder = new ProtocolEncoder(output);
            ProtocolDecoder decoder = new ProtocolDecoder(input);

            FetchRequest fetch =
                    new FetchRequest("recovery", 0, producedOffset);

            encoder.writeFrame(new ProtocolFrame(
                    ProtocolFrame.CURRENT_VERSION,
                    RequestType.FETCH,
                    2L,
                    fetch.encode()
            ));

            ProtocolFrame response = decoder.readFrame();

            assertEquals(RequestType.FETCH, response.getRequestType());

            ByteBuffer buffer =
                    ByteBuffer.wrap(response.getPayload());

            assertEquals(producedOffset, buffer.getLong());

            int length = buffer.getInt();
            byte[] message = new byte[length];
            buffer.get(message);

            assertArrayEquals(
                    "survives restart".getBytes(StandardCharsets.UTF_8),
                    message
            );
        } finally {
            secondBroker.shutdown();
        }
    }

    private KafkaBroker startBroker(Path dataDirectory)
            throws Exception {

        KafkaBroker broker =
                new KafkaBroker(0, 2, dataDirectory);

        Thread thread = new Thread(() -> {
            try {
                broker.start();
            } catch (Exception ignored) {
            }
        });

        thread.start();

        long deadline = System.currentTimeMillis() + 2000;

        while (broker.getPort() <= 0
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        if (broker.getPort() <= 0) {
            broker.shutdown();
            thread.join(1000);
            throw new IllegalStateException(
                    "Broker did not start"
            );
        }

        return broker;
    }
}
