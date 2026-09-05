package com.kafka.broker;

import com.kafka.protocol.FetchRequest;
import com.kafka.protocol.ProduceRequest;
import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientConnection implements Runnable {

    private final int connectionId;
    private final Socket socket;
    private final TopicManager topicManager;

    public ClientConnection(int connectionId, Socket socket, TopicManager topicManager) {

        this.connectionId = connectionId;
        this.socket = socket;
        this.topicManager = topicManager;
    }

    @Override
    public void run() {

        System.out.println("Connection #"
                        + connectionId
                        + " handled by "
                        + Thread.currentThread().getName());

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

                System.out.println("Connection #"
                                + connectionId
                                + " received "
                                + request.getRequestType()
                                + " request");

                handleRequest(request, encoder);
            }

        } catch (IOException e) {

            System.out.println("Connection #"
                            + connectionId
                            + " closed: "
                            + e.getMessage());

        } finally {

            System.out.println("Connection #"
                            + connectionId
                            + " worker finished.");
        }
    }

    private void handleRequest(
            ProtocolFrame request,
            ProtocolEncoder encoder)
            throws IOException {

        switch (request.getRequestType()) {

            case PING -> handlePing(request, encoder);
            case PRODUCE -> handleProduce(request, encoder);
            case FETCH -> handleFetch(request, encoder);
        }
    }

    private void handlePing(
            ProtocolFrame request,
            ProtocolEncoder encoder)
            throws IOException {

        byte[] payload = "PONG".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        ProtocolFrame response = new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        RequestType.PING,
                        request.getCorrelationId(),
                        payload);

        encoder.writeFrame(response);
    }

    private void handleProduce(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        try {
            ProduceRequest produceRequest = ProduceRequest.decode(request.getPayload());

            long offset = topicManager.produce(
                            produceRequest.getTopic(),
                            produceRequest.getPartition(),
                            produceRequest.getPayload());

            byte[] responsePayload = ByteBuffer
                            .allocate(Long.BYTES)
                            .putLong(offset)
                            .array();

            ProtocolFrame response = new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.PRODUCE,
                            request.getCorrelationId(),
                            responsePayload);

            encoder.writeFrame(response);

            System.out.println("Produced message to topic="
                            + produceRequest.getTopic()
                            + ", partition="
                            + produceRequest.getPartition()
                            + ", offset="
                            + offset);

        } catch (IllegalArgumentException e) {

            System.out.println("Invalid PRODUCE request: " + e.getMessage());
        }
    }

    private void handleFetch(
            ProtocolFrame request,
            ProtocolEncoder encoder)
            throws IOException {

        try {

            FetchRequest fetchRequest = FetchRequest.decode(request.getPayload());

            FetchResult result = topicManager.fetch(
                            fetchRequest.getTopic(),
                            fetchRequest.getPartition(),
                            fetchRequest.getOffset());

            byte[] responsePayload;

            if (result == null) {
                responsePayload = ByteBuffer
                                .allocate(Long.BYTES)
                                .putLong(-1L)
                                .array();

            } else {

                byte[] message = result.getPayload();

                responsePayload = ByteBuffer
                                .allocate(
                                        Long.BYTES
                                                + Integer.BYTES
                                                + message.length)
                                .putLong(result.getOffset())
                                .putInt(message.length)
                                .put(message)
                                .array();
            }

            ProtocolFrame response = new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.FETCH,
                            request.getCorrelationId(),
                            responsePayload);

            encoder.writeFrame(response);

            System.out.println("Fetched message from topic="
                            + fetchRequest.getTopic()
                            + ", partition="
                            + fetchRequest.getPartition()
                            + ", offset="
                            + fetchRequest.getOffset());

        } catch (IllegalArgumentException e) {

            System.out.println("Invalid FETCH request: " + e.getMessage());
        }
    }
}