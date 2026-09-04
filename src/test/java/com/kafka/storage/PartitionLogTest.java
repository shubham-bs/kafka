package com.kafka.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogTest {

    @Test
    void shouldAssignMonotonicOffsets() throws Exception {

        Path tempDirectory = Files.createTempDirectory("partition-log-test");

        try (PartitionLog log = new PartitionLog(tempDirectory)) {
            assertEquals(0, log.append("first"));
            assertEquals(1, log.append("second"));
            assertEquals(2, log.append("third"));
            assertEquals(3, log.nextOffset());
            assertEquals(3, log.size());
        }
    }


    @Test
    void shouldRecoverStateAfterRestart() throws Exception {

        Path tempDirectory = Files.createTempDirectory("partition-recovery-test");

        try (PartitionLog log = new PartitionLog(tempDirectory)) {
            assertEquals(0, log.append("first"));
            assertEquals(1, log.append("second"));
            assertEquals(2, log.append("third"));
            assertEquals(3, log.nextOffset());
        }

        try (PartitionLog recoveredLog = new PartitionLog(tempDirectory)) {

            assertEquals(3, recoveredLog.nextOffset());
            assertEquals(3, recoveredLog.size());
            assertEquals(3, recoveredLog.append("fourth"));
            assertEquals(4, recoveredLog.nextOffset());
            assertEquals(4, recoveredLog.size());
        }
    }

    @Test
    void shouldReadRecordByOffset() throws Exception {

        Path tempDirectory = Files.createTempDirectory("partition-read-test");

        try (PartitionLog log = new PartitionLog(tempDirectory)) {
            log.append("first");
            log.append("second");
            log.append("third");
            LogRecord record = log.read(1);

            assertEquals(1, record.getOffset());
            assertEquals("second", new String(
                            record.getPayload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldRejectUnknownOffset() throws Exception {

        Path tempDirectory = Files.createTempDirectory("partition-read-test");

        try (PartitionLog log = new PartitionLog(tempDirectory)) {
            log.append("first");
            assertThrows(IllegalArgumentException.class, () -> log.read(99));
        }
    }

    @Test
    void shouldPersistRecordAfterAppend() throws Exception {

        Path tempDirectory = Files.createTempDirectory("partition-durability-test");

        try (PartitionLog log = new PartitionLog(tempDirectory)) {
            assertEquals(0, log.append("durable-message"));
        }

        try (PartitionLog recoveredLog = new PartitionLog(tempDirectory)) {
            assertEquals(1, recoveredLog.size());
            assertEquals(1, recoveredLog.nextOffset());

            LogRecord record = recoveredLog.read(0);

            assertEquals(0, record.getOffset());
            assertEquals("durable-message",
                    new String(
                            record.getPayload(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }
}