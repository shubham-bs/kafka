package com.kafka.broker;

import com.kafka.protocol.FetchRequest;
import com.kafka.protocol.ProduceRequest;
import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.ReplicateRequest;
import com.kafka.protocol.RequestType;
import com.kafka.storage.LogRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientConnection implements Runnable {

    private final int connectionId;
    private final Socket socket;
    private final TopicManager topicManager;
    private final ReplicationManager replicationManager;
    private final ClusterMetadata clusterMetadata;

    public ClientConnection(
            int connectionId,
            Socket socket,
            TopicManager topicManager,
            ReplicationManager replicationManager,
            ClusterMetadata clusterMetadata) {

        this.connectionId = connectionId;
        this.socket = socket;
        this.topicManager = topicManager;
        this.replicationManager = replicationManager;
        this.clusterMetadata = clusterMetadata;
    }

    public ClientConnection(int connectionId, Socket socket, TopicManager topicManager) {

        this(
                connectionId,
                socket,
                topicManager,
                null,
                null
        );
    }

    @Override
    public void run() {

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

                handleRequest(request, encoder);
            }

        } catch (IOException e) {

            System.out.println("Connection #"
                            + connectionId
                            + " closed: "
                            + e.getMessage());
        }
    }

    private void handleRequest(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        switch (request.getRequestType()) {
            case PING -> handlePing(request, encoder);
            case PRODUCE -> handleProduce(request, encoder);
            case FETCH -> handleFetch(request, encoder);
            case REPLICATE -> handleReplicate(request, encoder);
        }
    }

    private void handlePing(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        byte[] payload = "PONG".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        encoder.writeFrame(new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        RequestType.PING,
                        request.getCorrelationId(),
                        payload));
    }

    private void handleProduce(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        try {
            ProduceRequest produceRequest = ProduceRequest.decode(request.getPayload());

            if (replicationManager == null || clusterMetadata == null) {

                sendProduceResponse(
                        request,
                        encoder,
                        topicManager.produce(
                                produceRequest.getTopic(),
                                produceRequest.getPartition(),
                                produceRequest.getPayload()
                        )
                );

                return;
            }

            PartitionMetadata metadata = clusterMetadata.getPartition(
                            produceRequest.getTopic(),
                            produceRequest.getPartition()
                    );

            if (metadata == null) {

                sendProduceResponse(
                        request,
                        encoder,
                        topicManager.produce(
                                produceRequest.getTopic(),
                                produceRequest.getPartition(),
                                produceRequest.getPayload()
                        )
                );

                return;
            }

            int leaderId = metadata.getLeaderBrokerId();

            if (leaderId != replicationManager.getBrokerId()) {

                ProtocolFrame response =
                        forwardToLeader(
                                metadata,
                                request
                        );

                encoder.writeFrame(response);
                return;
            }

            long offset = replicationManager.produceLocally(
                            produceRequest.getTopic(),
                            produceRequest.getPartition(),
                            produceRequest.getPayload()
                    );

            replicationManager.replicate(
                    produceRequest.getTopic(),
                    produceRequest.getPartition(),
                    offset,
                    produceRequest.getPayload()
            );

            sendProduceResponse(
                    request,
                    encoder,
                    offset
            );

        } catch (IllegalArgumentException | IllegalStateException e) {

            sendErrorResponse(
                    request,
                    encoder,
                    e.getMessage()
            );
        }
    }

    private void handleFetch(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        try {

            FetchRequest fetchRequest = FetchRequest.decode(request.getPayload());

            if (replicationManager == null || clusterMetadata == null) {

                sendFetchResponse(
                        request,
                        encoder,
                        topicManager.fetch(
                                fetchRequest.getTopic(),
                                fetchRequest.getPartition(),
                                fetchRequest.getOffset()
                        )
                );

                return;
            }

            PartitionMetadata metadata = clusterMetadata.getPartition(
                            fetchRequest.getTopic(),
                            fetchRequest.getPartition()
                    );

            if (metadata == null) {

                sendFetchResponse(
                        request,
                        encoder,
                        topicManager.fetch(
                                fetchRequest.getTopic(),
                                fetchRequest.getPartition(),
                                fetchRequest.getOffset()
                        )
                );

                return;
            }

            if (metadata.getLeaderBrokerId() != replicationManager.getBrokerId()) {

                ProtocolFrame response =
                        forwardToLeader(
                                metadata,
                                request
                        );

                encoder.writeFrame(response);
                return;
            }

            LogRecord record = replicationManager.fetchLocally(
                            fetchRequest.getTopic(),
                            fetchRequest.getPartition(),
                            fetchRequest.getOffset()
                    );

            if (record == null) {
                sendFetchResponse(
                        request,
                        encoder,
                        null
                );

            } else {
                sendFetchResponse(
                        request,
                        encoder,
                        new FetchResult(
                                fetchRequest.getOffset(),
                                record.getPayload()
                        )
                );
            }

        } catch (IllegalArgumentException | IllegalStateException e) {

            sendErrorResponse(
                    request,
                    encoder,
                    e.getMessage()
            );
        }
    }

    private void handleReplicate(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {

        try {
            if (replicationManager == null) {
                throw new IllegalStateException("Replication is not enabled");
            }

            ReplicateRequest replicateRequest = ReplicateRequest.decode(request.getPayload());

            replicationManager.appendReplica(
                    replicateRequest.getTopic(),
                    replicateRequest.getPartition(),
                    replicateRequest.getOffset(),
                    replicateRequest.getPayload()
            );

            byte[] responsePayload = ByteBuffer
                            .allocate(Long.BYTES)
                            .putLong(
                                    replicateRequest.getOffset()
                            )
                            .array();

            encoder.writeFrame(new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.REPLICATE,
                            request.getCorrelationId(),
                            responsePayload
                    )
            );

        } catch (IllegalArgumentException | IllegalStateException e) {

            sendErrorResponse(
                    request,
                    encoder,
                    e.getMessage()
            );
        }
    }

    private ProtocolFrame forwardToLeader(PartitionMetadata metadata, ProtocolFrame request)
            throws IOException {

        int leaderId = metadata.getLeaderBrokerId();

        BrokerInfo leader = clusterMetadata.getBroker(leaderId);

        if (leader == null) {
            throw new IOException("Leader broker not found: " + leaderId);
        }

        if (leader.getPort() <= 0) {
            throw new IOException("Leader broker is not running: " + leaderId);
        }

        return BrokerClient.request(leader, request);
    }

    private void sendProduceResponse(
            ProtocolFrame request,
            ProtocolEncoder encoder,
            long offset)
            throws IOException {

        byte[] payload = ByteBuffer
                        .allocate(Long.BYTES)
                        .putLong(offset)
                        .array();

        encoder.writeFrame(new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        RequestType.PRODUCE,
                        request.getCorrelationId(),
                        payload
                )
        );
    }

    private void sendFetchResponse(
            ProtocolFrame request,
            ProtocolEncoder encoder,
            FetchResult result)
            throws IOException {

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
                                            + message.length
                            )
                            .putLong(result.getOffset())
                            .putInt(message.length)
                            .put(message)
                            .array();
        }

        encoder.writeFrame(new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        RequestType.FETCH,
                        request.getCorrelationId(),
                        responsePayload
                )
        );
    }

    private void sendErrorResponse(
            ProtocolFrame request,
            ProtocolEncoder encoder,
            String message)
            throws IOException {

        byte[] payload = message.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        encoder.writeFrame(new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        request.getRequestType(),
                        request.getCorrelationId(),
                        payload
                )
        );
    }
}