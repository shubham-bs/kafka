package com.kafka.broker;

public class FetchResult {

    private final long offset;
    private final byte[] payload;

    public FetchResult(long offset, byte[] payload) {

        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        this.offset = offset;
        this.payload = payload;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getPayload() {
        return payload;
    }
}