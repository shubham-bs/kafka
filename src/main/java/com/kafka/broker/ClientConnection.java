package com.kafka.broker;

import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ClientConnection implements Runnable {

    private final int connectionId;
    private final Socket socket;

    public ClientConnection(int connectionId, Socket socket) {
        this.connectionId = connectionId;
        this.socket = socket;
    }

    @Override
    public void run() {
        System.out.println("Connection #" + connectionId + " handled by "
                + Thread.currentThread().getName()
        );

        try (
                Socket client = socket;
                InputStream input = client.getInputStream();
                OutputStream output = client.getOutputStream()
        ) {
            ProtocolDecoder decoder = new ProtocolDecoder(input);

            ProtocolEncoder encoder = new ProtocolEncoder(output);

            while (true) {
                ProtocolFrame request;
                try {
                    request = decoder.readFrame();
                } catch (IOException e) {
                    break;
                }

                System.out.println("Connection #" + connectionId
                                + " received "
                                + request.getRequestType()
                                + " request"
                );

                handleRequest(request, encoder);
            }

        } catch (IOException e) {
            System.out.println("Connection #" + connectionId + " closed: " + e.getMessage());
        } finally {
            System.out.println("Connection #" + connectionId + " worker finished.");
        }
    }

    private void handleRequest(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        if (request.getRequestType() == RequestType.PING) {
            byte[] responsePayload = "PONG".getBytes(java.nio.charset.StandardCharsets.UTF_8);

            ProtocolFrame response = new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.PING,
                            request.getCorrelationId(),
                            responsePayload
                    );

            encoder.writeFrame(response);

            return;
        }

        System.out.println("Connection #" + connectionId + " received unsupported request: "
                + request.getRequestType()
        );
    }
}