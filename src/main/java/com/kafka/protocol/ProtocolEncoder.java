package com.kafka.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class ProtocolEncoder {

    private final OutputStream outputStream;

    public ProtocolEncoder(OutputStream outputStream) {
        this.outputStream = Objects.requireNonNull(outputStream, "Output stream cannot be null");
    }

    public void writeFrame(ProtocolFrame frame) throws IOException {

        Objects.requireNonNull(frame, "Frame cannot be null");

        byte[] encoded = frame.encode();

        int offset = 0;

        while (offset < encoded.length) {
            int remaining = encoded.length - offset;

            outputStream.write(encoded, offset, remaining);

            offset = encoded.length;
        }

        outputStream.flush();
    }
}