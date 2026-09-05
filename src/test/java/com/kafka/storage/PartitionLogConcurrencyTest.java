package com.kafka.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogConcurrencyTest {
    @Test
    void shouldAssignUniqueOffsetsUnderConcurrentAppends(@TempDir Path directory) throws Exception {
        try (PartitionLog log = new PartitionLog(directory)) {
            int threads = 8;
            int messagesPerThread = 50;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            Set<Long> offsets = java.util.concurrent.ConcurrentHashMap.newKeySet();

            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    for (int i = 0; i < messagesPerThread; i++) {
                        try {
                            offsets.add(log.append("message"));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }

            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(threads * messagesPerThread, offsets.size());
            assertEquals(threads * messagesPerThread, log.size());
            assertEquals(threads * messagesPerThread, log.nextOffset());

            Set<Long> expected = new HashSet<>();
            for (long i = 0; i < threads * messagesPerThread; i++) expected.add(i);
            assertEquals(expected, offsets);
        }
    }
}
