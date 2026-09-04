package com.kafka.protocol;

import java.nio.ByteBuffer;

public class ProtocolFrame {

    /*
     * Frame layout:
     * 4 bytes  -> total frame length after this field
     * 4 bytes  -> protocol version
     * 4 bytes  -> request type
     * 8 bytes  -> correlation/request ID
     * N bytes  -> payload
     */
    private static final int LENGTH_FIELD_SIZE = Integer.BYTES;
    private static final int VERSION_SIZE = Integer.BYTES;
    private static final int REQUEST_TYPE_SIZE = Integer.BYTES;
    private static final int CORRELATION_ID_SIZE = Long.BYTES;

    public static final int HEADER_SIZE = LENGTH_FIELD_SIZE
                    + VERSION_SIZE
                    + REQUEST_TYPE_SIZE
                    + CORRELATION_ID_SIZE;

    public static final int CURRENT_VERSION = 1;

    public static final int MAX_FRAME_SIZE = 1024 * 1024;

    private final int version;
    private final RequestType requestType;
    private final long correlationId;
    private final byte[] payload;

    public ProtocolFrame(
            int version,
            RequestType requestType,
            long correlationId,
            byte[] payload) {

        if (version <= 0) {
            throw new IllegalArgumentException("Version must be positive");
        }
        if (requestType == null) {
            throw new IllegalArgumentException("Request type cannot be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        this.version = version;
        this.requestType = requestType;
        this.correlationId = correlationId;
        this.payload = payload;
    }

    public int getVersion() {
        return version;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int payloadSize() {
        return payload.length;
    }

    public int frameSize() {
        return VERSION_SIZE + REQUEST_TYPE_SIZE + CORRELATION_ID_SIZE + payload.length;
    }

    public byte[] encode() {

        int frameSize = frameSize();

        if (frameSize > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Frame exceeds maximum size: " + frameSize);
        }

        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_FIELD_SIZE + frameSize);

        buffer.putInt(frameSize);
        buffer.putInt(version);
        buffer.putInt(requestType.getCode());
        buffer.putLong(correlationId);
        buffer.put(payload);

        return buffer.array();
    }

    public static ProtocolFrame decode(byte[] encoded) {

        if (encoded == null) {
            throw new IllegalArgumentException("Encoded frame cannot be null");
        }
        if (encoded.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Frame too small");
        }

        ByteBuffer buffer = ByteBuffer.wrap(encoded);

        int declaredFrameSize = buffer.getInt();

        if (declaredFrameSize != encoded.length - LENGTH_FIELD_SIZE) {
            throw new IllegalArgumentException("Frame length mismatch");
        }
        if (declaredFrameSize > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Frame exceeds maximum size");
        }

        int version = buffer.getInt();

        RequestType requestType = RequestType.fromCode(buffer.getInt());

        long correlationId = buffer.getLong();

        byte[] payload = new byte[buffer.remaining()];

        buffer.get(payload);

        return new ProtocolFrame(version, requestType, correlationId, payload);
    }
}