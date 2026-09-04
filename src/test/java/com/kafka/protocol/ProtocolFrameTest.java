package com.kafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolFrameTest {

    @Test
    void shouldEncodeAndDecodeFrame() {

        byte[] payload = "hello kafka".getBytes(StandardCharsets.UTF_8);

        ProtocolFrame original = new ProtocolFrame(1, RequestType.PING, 42, payload);

        byte[] encoded = original.encode();

        ProtocolFrame decoded = ProtocolFrame.decode(encoded);

        assertEquals(1, decoded.getVersion());

        assertEquals(RequestType.PING, decoded.getRequestType());

        assertEquals(42, decoded.getCorrelationId());

        assertArrayEquals(payload, decoded.getPayload());
    }

    @Test
    void shouldRejectFrameWithIncorrectLength() {

        ProtocolFrame frame = new ProtocolFrame(1, RequestType.PING, 1, new byte[0]);

        byte[] encoded = frame.encode();

        encoded[3]++;

        assertThrows(IllegalArgumentException.class, () -> ProtocolFrame.decode(encoded));
    }

    @Test
    void shouldRejectUnknownRequestType() {

        ProtocolFrame frame = new ProtocolFrame(1, RequestType.PING, 1, new byte[0]);

        byte[] encoded = frame.encode();

        encoded[11] = 99;

        assertThrows(IllegalArgumentException.class, () -> ProtocolFrame.decode(encoded));
    }

    @Test
    void shouldRejectOversizedPayload() {

        byte[] payload = new byte[ProtocolFrame.MAX_FRAME_SIZE];

        ProtocolFrame frame = new ProtocolFrame(1, RequestType.PING, 1, payload);

        assertThrows(IllegalArgumentException.class, frame::encode);
    }
}