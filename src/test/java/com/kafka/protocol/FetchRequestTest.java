package com.kafka.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FetchRequestTest {

    @Test
    void shouldEncodeAndDecodeRequest() {

        FetchRequest original = new FetchRequest("orders", 2, 17);

        byte[] encoded = original.encode();

        FetchRequest decoded = FetchRequest.decode(encoded);

        assertEquals("orders", decoded.getTopic());
        assertEquals(2, decoded.getPartition());
        assertEquals(17L, decoded.getOffset());
    }

    @Test
    void shouldRejectNegativePartition() {

        assertThrows(IllegalArgumentException.class, () -> new FetchRequest(
                "orders",
                -1,
                0));
    }

    @Test
    void shouldRejectNegativeOffset() {

        assertThrows(IllegalArgumentException.class, () -> new FetchRequest(
                        "orders",
                        0,
                        -1));
    }

    @Test
    void shouldRejectTrailingBytes() {

        FetchRequest request = new FetchRequest(
                        "orders",
                        0,
                        0);

        byte[] encoded = request.encode();

        byte[] malformed = new byte[encoded.length + 1];

        System.arraycopy(encoded,
                0, malformed,
                0, encoded.length);

        assertThrows(IllegalArgumentException.class, () -> FetchRequest.decode(malformed));
    }
}