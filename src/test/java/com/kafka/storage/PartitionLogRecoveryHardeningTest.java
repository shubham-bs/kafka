package com.kafka.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogRecoveryHardeningTest {
    @Test
    void shouldTruncateIncompleteTailAfterRestart(@TempDir Path directory) throws Exception {
        try (PartitionLog log = new PartitionLog(directory)) {
            assertEquals(0, log.append("first"));
            assertEquals(1, log.append("second"));
        }

        Path segment = directory.resolve("00000000000000000000.log");
        Files.write(segment, new byte[]{0, 0, 0}, StandardOpenOption.APPEND);

        try (PartitionLog recovered = new PartitionLog(directory)) {
            assertEquals(2, recovered.size());
            assertEquals(2, recovered.nextOffset());
            assertEquals("first", new String(recovered.read(0).getPayload(), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals(2, recovered.append("third"));
        }
    }
}
