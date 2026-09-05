package com.kafka.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class ProtocolDecoder {

    private final InputStream inputStream;

    public ProtocolDecoder(InputStream inputStream) {
        this.inputStream = Objects.requireNonNull(
                inputStream,
                "Input stream cannot be null"
        );
    }

    public ProtocolFrame readFrame() throws IOException {
        byte[] lengthBytes = readExactly(Integer.BYTES);

        int frameSize =
                ((lengthBytes[0] & 0xFF) << 24)
                        | ((lengthBytes[1] & 0xFF) << 16)
                        | ((lengthBytes[2] & 0xFF) << 8)
                        | (lengthBytes[3] & 0xFF);

        if (frameSize < Integer.BYTES + Integer.BYTES + Long.BYTES) {
            throw new IOException("Invalid frame size: " + frameSize);
        }

        if (frameSize > ProtocolFrame.MAX_FRAME_SIZE) {
            throw new IOException(
                    "Frame exceeds maximum size: " + frameSize
            );
        }

        byte[] frameBody = readExactly(frameSize);

        byte[] completeFrame = new byte[Integer.BYTES + frameSize];

        System.arraycopy(
                lengthBytes,
                0,
                completeFrame,
                0,
                Integer.BYTES
        );

        System.arraycopy(
                frameBody,
                0,
                completeFrame,
                Integer.BYTES,
                frameSize
        );

        try {
            return ProtocolFrame.decode(completeFrame);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid protocol frame", e);
        }
    }

    private byte[] readExactly(int length) throws IOException {
        byte[] buffer = new byte[length];
        int totalRead = 0;

        while (totalRead < length) {
            int bytesRead =
                    inputStream.read(
                            buffer,
                            totalRead,
                            length - totalRead
                    );

            if (bytesRead == -1) {
                if (totalRead == 0) {
                    throw new EOFException("Connection closed");
                }

                throw new EOFException(
                        "Connection closed while reading frame"
                );
            }

            if (bytesRead == 0) {
                int singleByte = inputStream.read();

                if (singleByte == -1) {
                    if (totalRead == 0) {
                        throw new EOFException("Connection closed");
                    }

                    throw new EOFException(
                            "Connection closed while reading frame"
                    );
                }

                buffer[totalRead++] = (byte) singleByte;
                continue;
            }

            totalRead += bytesRead;
        }

        return buffer;
    }
}
