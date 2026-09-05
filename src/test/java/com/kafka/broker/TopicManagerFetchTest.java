package com.kafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TopicManagerFetchTest {

    @Test
    void shouldFetchProducedMessage(@TempDir Path tempDirectory) throws Exception {

        TopicManager topicManager = new TopicManager(tempDirectory);

        try {
            byte[] first = "first".getBytes(StandardCharsets.UTF_8);

            byte[] second = "second".getBytes(StandardCharsets.UTF_8);

            assertEquals(0L, topicManager.produce(
                            "orders",
                            0,
                            first));

            assertEquals(1L, topicManager.produce(
                            "orders",
                            0,
                            second));

            FetchResult result = topicManager.fetch(
                            "orders",
                            0,
                            1L);

            assertNotNull(result);

            assertEquals(1L, result.getOffset());

            assertArrayEquals(second, result.getPayload());

        } finally {
            topicManager.close();
        }
    }

    @Test
    void shouldReturnNullAtEndOfPartition(@TempDir Path tempDirectory) throws Exception {

        TopicManager topicManager = new TopicManager(tempDirectory);

        try {

            topicManager.produce(
                    "orders",
                    0,
                    "hello"
                            .getBytes(StandardCharsets.UTF_8));

            FetchResult result = topicManager.fetch(
                            "orders",
                            0,
                            1L);

            assertNull(result);

        } finally {
            topicManager.close();
        }
    }
}