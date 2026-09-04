package com.kafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolDecoderTest {

    @Test
    void shouldDecodeCompleteFrame() throws Exception {

        ProtocolFrame frame = new ProtocolFrame(
                        1,
                        RequestType.PING,
                        100,
                        "hello".getBytes(
                                StandardCharsets.UTF_8
                        )
                );

        ByteArrayInputStream input = new ByteArrayInputStream(frame.encode());

        ProtocolDecoder decoder = new ProtocolDecoder(input);

        ProtocolFrame decoded = decoder.readFrame();

        assertEquals(RequestType.PING, decoded.getRequestType());

        assertEquals(100, decoded.getCorrelationId());
    }

    @Test
    void shouldHandleMultipleFramesInOneStream() throws Exception {

        ProtocolFrame first = new ProtocolFrame(
                        1,
                        RequestType.PING,
                        1,
                        "first".getBytes(StandardCharsets.UTF_8)
                );

        ProtocolFrame second = new ProtocolFrame(
                        1,
                        RequestType.PING,
                        2,
                        "second".getBytes(StandardCharsets.UTF_8)
                );

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        output.write(first.encode());
        output.write(second.encode());

        ProtocolDecoder decoder = new ProtocolDecoder(new ByteArrayInputStream(output.toByteArray()));

        ProtocolFrame decodedFirst = decoder.readFrame();

        ProtocolFrame decodedSecond = decoder.readFrame();

        assertEquals(1, decodedFirst.getCorrelationId());

        assertEquals(2, decodedSecond.getCorrelationId());
    }

    @Test
    void shouldHandleFragmentedFrame() throws Exception {

        ProtocolFrame original = new ProtocolFrame(
                        1,
                        RequestType.PING,
                        55,
                        "fragmented".getBytes(StandardCharsets.UTF_8)
                );

        byte[] encoded = original.encode();

        ByteArrayInputStream input = new ByteArrayInputStream(encoded) {

                    @Override
                    public synchronized int read(byte[] buffer, int offset, int length) {
                        return super.read(buffer, offset, Math.min(length, 2));
                    }
                };

        ProtocolDecoder decoder = new ProtocolDecoder(input);

        ProtocolFrame decoded = decoder.readFrame();

        assertEquals(55, decoded.getCorrelationId());

        assertArrayEquals(original.getPayload(), decoded.getPayload());
    }
}