package com.kafka.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ReplicateRequest {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] payload;

    public ReplicateRequest(
            String topic,
            int partition,
            long offset,
            byte[] payload) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "Partition cannot be negative"
            );
        }

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative"
            );
        }

        if (payload == null) {
            throw new IllegalArgumentException(
                    "Payload cannot be null"
            );
        }

        byte[] topicBytes =
                topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length > 1024) {
            throw new IllegalArgumentException(
                    "Topic name too long"
            );
        }

        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.payload = payload.clone();
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

    public byte[] getPayload() {
        return payload.clone();
    }

    public byte[] encode() {

        byte[] topicBytes =
                topic.getBytes(StandardCharsets.UTF_8);

        return ByteBuffer
                .allocate(
                        Integer.BYTES
                                + topicBytes.length
                                + Integer.BYTES
                                + Long.BYTES
                                + Integer.BYTES
                                + payload.length
                )
                .putInt(topicBytes.length)
                .put(topicBytes)
                .putInt(partition)
                .putLong(offset)
                .putInt(payload.length)
                .put(payload)
                .array();
    }

    public static ReplicateRequest decode(byte[] bytes) {

        if (bytes == null) {
            throw new IllegalArgumentException(
                    "Payload cannot be null"
            );
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException(
                    "Invalid REPLICATE request"
            );
        }

        int topicLength = buffer.getInt();

        if (topicLength <= 0 || topicLength > 1024) {
            throw new IllegalArgumentException(
                    "Invalid topic length"
            );
        }

        if (buffer.remaining()
                < topicLength
                + Integer.BYTES
                + Long.BYTES
                + Integer.BYTES) {

            throw new IllegalArgumentException(
                    "Invalid REPLICATE request"
            );
        }

        byte[] topicBytes = new byte[topicLength];

        buffer.get(topicBytes);

        String topic =
                new String(
                        topicBytes,
                        StandardCharsets.UTF_8
                );

        int partition = buffer.getInt();
        long offset = buffer.getLong();

        int payloadLength = buffer.getInt();

        if (payloadLength < 0
                || payloadLength > buffer.remaining()) {

            throw new IllegalArgumentException(
                    "Invalid payload length"
            );
        }

        byte[] payload = new byte[payloadLength];

        buffer.get(payload);

        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException(
                    "Trailing bytes in REPLICATE request"
            );
        }

        return new ReplicateRequest(
                topic,
                partition,
                offset,
                payload
        );
    }
}