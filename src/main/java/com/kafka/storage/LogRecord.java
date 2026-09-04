package com.kafka.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Represents one record stored in append-only log.
 *
 * A record contains:
 *
 *      offset  -> unique sequential position in the log
 *      payload -> actual message data
 *
 * The record itself does NOT know anything about files.
 * File management belongs to LogSegment.
 */
public class LogRecord {

    private final long offset;
    private final byte[] payload;


    public LogRecord(long offset, byte[] payload) {

        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        this.offset = offset;
        this.payload = payload;
    }


    /**
     * Convenience constructor for String messages.
     */
    public LogRecord(long offset, String payload) {
        this(offset, payload.getBytes(StandardCharsets.UTF_8));
    }


    public long getOffset() {
        return offset;
    }


    public byte[] getPayload() {
        return payload;
    }


    /**
     * Returns the number of bytes required to serialize
     * this record.
     *
     * Layout:
     *
     * 4 bytes  -> record length
     * 8 bytes  -> offset
     * 4 bytes  -> payload length
     * N bytes  -> payload
     */
    public int serializedSize() {

        return Integer.BYTES       // record length
                + Long.BYTES        // offset
                + Integer.BYTES     // payload length
                + payload.length;   // actual payload
    }


    /**
     * Serializes the record into a byte array.
     */
    public byte[] serialize() {

        ByteBuffer buffer = ByteBuffer.allocate(serializedSize());

        /*
         * Total record size excluding the record-length
         * field itself.
         */
        int recordBodyLength = Long.BYTES
                        + Integer.BYTES
                        + payload.length;


        // [recordLength]
        buffer.putInt(recordBodyLength);

        // [offset]
        buffer.putLong(offset);

        // [payloadLength]
        buffer.putInt(payload.length);

        // [payload]
        buffer.put(payload);

        return buffer.array();
    }


    /**
     * Reconstructs a LogRecord from serialized bytes.
     */
    public static LogRecord deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int recordBodyLength = buffer.getInt();

        if (recordBodyLength != data.length - Integer.BYTES) {
            throw new IllegalArgumentException("Invalid record length: " + recordBodyLength);
        }

        // [offset]
        long offset = buffer.getLong();

        // [payloadLength]
        int payloadLength = buffer.getInt();

        if (payloadLength < 0 || payloadLength > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid payload length: " + payloadLength);
        }

        // [payload]
        byte[] payload = new byte[payloadLength];

        buffer.get(payload);

        return new LogRecord(offset, payload);
    }


    @Override
    public String toString() {

        return "LogRecord{" +
                "offset=" + offset +
                ", payload=" +
                new String(payload, StandardCharsets.UTF_8) +
                '}';
    }
}