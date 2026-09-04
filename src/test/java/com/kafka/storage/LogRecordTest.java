package com.kafka.storage;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogRecordTest {

    @Test
    void shouldSerializeAndDeserializeRecord() {
        LogRecord original = new LogRecord(42, "hello kafka");

        byte[] serialized = original.serialize();

        LogRecord restored = LogRecord.deserialize(serialized);

        assertEquals(original.getOffset(), restored.getOffset());

        assertArrayEquals(original.getPayload(), restored.getPayload());
    }
}