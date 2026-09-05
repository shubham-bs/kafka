package com.kafka.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FetchRequest {

    private static final int TOPIC_LENGTH_SIZE = Integer.BYTES;
    private static final int PARTITION_SIZE = Integer.BYTES;
    private static final int OFFSET_SIZE = Long.BYTES;

    private static final int MAX_TOPIC_LENGTH = 255;

    private final String topic;
    private final int partition;
    private final long offset;

    public FetchRequest(String topic, int partition, long offset) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("Topic name too long");
        }

        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] encode() {

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        int size = TOPIC_LENGTH_SIZE
                        + topicBytes.length
                        + PARTITION_SIZE
                        + OFFSET_SIZE;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);

        return buffer.array();
    }

    public static FetchRequest decode(byte[] encoded) {

        if (encoded == null) {
            throw new IllegalArgumentException("Encoded fetch request cannot be null");
        }

        if (encoded.length < TOPIC_LENGTH_SIZE + PARTITION_SIZE + OFFSET_SIZE) {
            throw new IllegalArgumentException("Fetch request too small");
        }

        ByteBuffer buffer = ByteBuffer.wrap(encoded);

        int topicLength = buffer.getInt();

        if (topicLength <= 0 || topicLength > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("Invalid topic length: " + topicLength);
        }

        if (buffer.remaining() < topicLength + PARTITION_SIZE + OFFSET_SIZE) {
            throw new IllegalArgumentException("Malformed fetch request");
        }

        byte[] topicBytes = new byte[topicLength];

        buffer.get(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        int partition = buffer.getInt();

        long offset = buffer.getLong();

        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Unexpected trailing bytes");
        }

        return new FetchRequest(topic, partition, offset);
    }
}