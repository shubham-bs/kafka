package com.kafka.broker;

import com.kafka.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCreatePartitionReplica() throws Exception {
        ReplicationManager manager =
                new ReplicationManager(0, tempDir);

        assertDoesNotThrow(() ->
                manager.createPartitionReplica(
                        "orders",
                        0,
                        List.of(0, 1, 2),
                        0
                )
        );
    }

    @Test
    void shouldProduceLocally() throws Exception {
        ReplicationManager manager =
                new ReplicationManager(0, tempDir);

        manager.createPartitionReplica(
                "orders",
                0,
                List.of(0, 1, 2),
                0
        );

        long offset = manager.produceLocally(
                "orders",
                0,
                "hello".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals(0, offset);

        LogRecord record = manager.fetchLocally(
                "orders",
                0,
                0
        );

        assertNotNull(record);

        assertArrayEquals(
                "hello".getBytes(StandardCharsets.UTF_8),
                record.getPayload()
        );
    }

    @Test
    void shouldAppendReplicaLocally() throws Exception {
        ReplicationManager manager =
                new ReplicationManager(1, tempDir);

        manager.createPartitionReplica(
                "orders",
                0,
                List.of(0, 1, 2),
                0
        );

        manager.appendReplica(
                "orders",
                0,
                0,
                "hello".getBytes(StandardCharsets.UTF_8)
        );

        LogRecord record = manager.fetchLocally(
                "orders",
                0,
                0
        );

        assertNotNull(record);

        assertArrayEquals(
                "hello".getBytes(StandardCharsets.UTF_8),
                record.getPayload()
        );
    }

    @Test
    void shouldReplicateWithoutRunningFollowers() throws Exception {
        ReplicationManager manager =
                new ReplicationManager(0, tempDir);

        manager.createPartitionReplica(
                "orders",
                0,
                List.of(0, 1, 2),
                0
        );

        assertDoesNotThrow(() ->
                manager.replicate(
                        "orders",
                        0,
                        0,
                        "hello".getBytes(StandardCharsets.UTF_8)
                )
        );
    }
}