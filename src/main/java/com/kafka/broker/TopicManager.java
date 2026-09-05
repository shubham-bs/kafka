package com.kafka.broker;

import com.kafka.storage.LogRecord;
import com.kafka.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager implements AutoCloseable {

    private static final int DEFAULT_PARTITION_COUNT = 3;

    private final Path dataDirectory;

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    public TopicManager(Path dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        Files.createDirectories(dataDirectory);
    }

    public Topic getOrCreateTopic(String topicName) throws IOException {

        validateTopicName(topicName);

        Topic existing = topics.get(topicName);

        if (existing != null) return existing;

        synchronized (topics) {
            existing = topics.get(topicName);

            if (existing != null) return existing;

            Path topicDirectory = dataDirectory.resolve(topicName);

            Topic created = new Topic(topicName, DEFAULT_PARTITION_COUNT, topicDirectory);

            topics.put(topicName, created);

            return created;
        }
    }

    public long produce(String topicName, int partition, byte[] payload) throws IOException {

        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        Topic topic = getOrCreateTopic(topicName);

        return topic.getPartition(partition).append(payload);
    }

    public FetchResult fetch(String topicName, int partition, long offset) throws IOException {

        Topic topic = getOrCreateTopic(topicName);

        PartitionLog partitionLog = topic.getPartition(partition);

        if (offset >= partitionLog.nextOffset()) return null;

        LogRecord record = partitionLog.read(offset);

        return new FetchResult(record.getOffset(), record.getPayload());
    }

    public int topicCount() {
        return topics.size();
    }

    private void validateTopicName(String topicName) {

        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        if (topicName.contains("/") || topicName.contains("\\") || topicName.contains("..")) {
            throw new IllegalArgumentException("Invalid topic name: " + topicName);
        }
    }

    @Override
    public void close() throws IOException {

        IOException firstException = null;

        for (Topic topic : topics.values()) {

            try {
                topic.close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
            }
        }

        topics.clear();

        if (firstException != null) throw firstException;
    }
}