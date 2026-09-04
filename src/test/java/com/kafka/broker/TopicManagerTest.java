package com.kafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TopicManagerTest {

    @TempDir
    Path tempDirectory;

    @Test
    void shouldCreateTopicWithThreePartitions() throws Exception {

        try (TopicManager manager = new TopicManager(tempDirectory)) {

            Topic topic = manager.getOrCreateTopic("orders");

            assertEquals("orders", topic.getName());
            assertEquals(3, topic.partitionCount());
            assertEquals(1, manager.topicCount());
        }
    }

    @Test
    void shouldReturnSameTopicInstance()
            throws Exception {

        try (TopicManager manager = new TopicManager(tempDirectory)) {
            Topic first = manager.getOrCreateTopic("orders");
            Topic second = manager.getOrCreateTopic("orders");

            assertSame(first, second);
        }
    }

    @Test
    void shouldProduceMessageAndAssignOffset() throws Exception {
        try (TopicManager manager = new TopicManager(tempDirectory)) {
            long firstOffset = manager.produce("orders", 0,
                            "hello".getBytes(StandardCharsets.UTF_8));

            long secondOffset = manager.produce("orders", 0,
                            "world".getBytes(StandardCharsets.UTF_8));

            assertEquals(0, firstOffset);

            assertEquals(1, secondOffset);

            assertEquals(
                    2, manager.getOrCreateTopic("orders")
                            .getPartition(0)
                            .size());
        }
    }

    @Test
    void partitionsHaveIndependentOffsets() throws Exception {
        try (TopicManager manager = new TopicManager(tempDirectory)) {

            long partitionZeroOffset = manager.produce("orders", 0,
                            "A".getBytes(StandardCharsets.UTF_8));

            long partitionOneOffset = manager.produce("orders", 1,
                            "B".getBytes(StandardCharsets.UTF_8));

            assertEquals(0, partitionZeroOffset);
            assertEquals(0, partitionOneOffset);
        }
    }

    @Test
    void shouldRejectInvalidPartition() throws Exception {

        try (TopicManager manager = new TopicManager(tempDirectory)) {

            assertThrows(IllegalArgumentException.class, () -> manager.produce(
                            "orders", 10,
                            "hello".getBytes(StandardCharsets.UTF_8)));
        }
    }
}