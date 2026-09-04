package com.kafka.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Binary representation of a PRODUCE request.
 * Payload layout:
 *     4 bytes  -> topic name length
 *     N bytes  -> topic name (UTF-8)
 *     4 bytes  -> partition
 *     4 bytes  -> message length
 *     N bytes  -> message payload
 */
public class ProduceRequest {

    private static final int INTEGER_SIZE = Integer.BYTES;

    private static final int MAX_TOPIC_NAME_SIZE = 255;

    private static final int MAX_MESSAGE_SIZE = ProtocolFrame.MAX_FRAME_SIZE;

    private final String topic;
    private final int partition;
    private final byte[] payload;

    public ProduceRequest(String topic, int partition, byte[] payload) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be empty");
        }

        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative");
        }

        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length > MAX_TOPIC_NAME_SIZE) {
            throw new IllegalArgumentException("Topic name is too long");
        }

        if (payload.length > MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("Message is too large");
        }

        this.topic = topic;
        this.partition = partition;
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public byte[] getPayload() {
        return payload;
    }

    public byte[] encode() {

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        int size = INTEGER_SIZE
                        + topicBytes.length
                        + INTEGER_SIZE
                        + INTEGER_SIZE
                        + payload.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(partition);

        buffer.putInt(payload.length);
        buffer.put(payload);

        return buffer.array();
    }

    public static ProduceRequest decode(byte[] encoded) {

        if (encoded == null) {
            throw new IllegalArgumentException("Encoded request cannot be null");
        }

        ByteBuffer buffer = ByteBuffer.wrap(encoded);

        if (buffer.remaining() < INTEGER_SIZE) {
            throw new IllegalArgumentException("Missing topic length");
        }

        int topicLength = buffer.getInt();

        if (topicLength <= 0 || topicLength > MAX_TOPIC_NAME_SIZE || topicLength > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid topic length");
        }

        byte[] topicBytes = new byte[topicLength];

        buffer.get(topicBytes);

        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        if (buffer.remaining() < INTEGER_SIZE) {
            throw new IllegalArgumentException("Missing partition");
        }

        int partition = buffer.getInt();

        if (partition < 0) {
            throw new IllegalArgumentException("Invalid partition");
        }

        if (buffer.remaining() < INTEGER_SIZE) {
            throw new IllegalArgumentException("Missing message length");
        }

        int messageLength = buffer.getInt();

        if (messageLength < 0 || messageLength > MAX_MESSAGE_SIZE || messageLength > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid message length");
        }

        byte[] payload = new byte[messageLength];

        buffer.get(payload);

        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Unexpected bytes after PRODUCE request");
        }

        return new ProduceRequest(topic, partition, payload);
    }
}