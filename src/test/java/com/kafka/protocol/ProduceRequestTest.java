package com.kafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProduceRequestTest {

    @Test
    void shouldEncodeAndDecodeRequest() {

        byte[] payload = "hello kafka".getBytes(StandardCharsets.UTF_8);

        ProduceRequest original = new ProduceRequest("orders", 2, payload);

        byte[] encoded = original.encode();

        ProduceRequest decoded = ProduceRequest.decode(encoded);

        assertEquals("orders", decoded.getTopic());

        assertEquals(2, decoded.getPartition());

        assertArrayEquals(payload, decoded.getPayload());
    }

    @Test
    void shouldRejectNegativePartition() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceRequest("orders", -1, new byte[0]));
    }

    @Test
    void shouldRejectInvalidEncodedRequest() {

        byte[] invalid = new byte[]{0, 0, 0, 100};

        assertThrows(IllegalArgumentException.class, () -> ProduceRequest.decode(invalid));
    }
}