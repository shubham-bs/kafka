package com.kafka.broker;

import com.kafka.protocol.*;
import com.kafka.storage.LogRecord;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ClientConnection implements Runnable {
    private final int connectionId;
    private final Socket socket;
    private final TopicManager topicManager;
    private final ReplicationManager replicationManager;
    private final ClusterMetadata clusterMetadata;

    public ClientConnection(int connectionId, Socket socket, TopicManager topicManager,
                            ReplicationManager replicationManager, ClusterMetadata clusterMetadata) {
        this.connectionId = connectionId;
        this.socket = socket;
        this.topicManager = topicManager;
        this.replicationManager = replicationManager;
        this.clusterMetadata = clusterMetadata;
    }

    public ClientConnection(int connectionId, Socket socket, TopicManager topicManager) {
        this(connectionId, socket, topicManager, null, null);
    }

    @Override
    public void run() {
        try (Socket client = socket;
             InputStream input = client.getInputStream();
             OutputStream output = client.getOutputStream()) {

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
            System.out.println("Connection #" + connectionId + " closed: " + e.getMessage());
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
        write(request, encoder, RequestType.PING, "PONG".getBytes(StandardCharsets.UTF_8));
    }

    private void handleProduce(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {
        try {
            ProduceRequest r = ProduceRequest.decode(request.getPayload());

            if (!clustered()) {
                sendProduceResponse(request, encoder,
                        topicManager.produce(r.getTopic(), r.getPartition(), r.getPayload()));
                return;
            }

            PartitionMetadata metadata =
                    clusterMetadata.getPartition(r.getTopic(), r.getPartition());

            if (metadata == null) {
                sendProduceResponse(request, encoder,
                        topicManager.produce(r.getTopic(), r.getPartition(), r.getPayload()));
                return;
            }

            ensureLeaderAvailable(metadata);

            ProtocolFrame forwarded = forwardIfNeeded(metadata, request);
            if (forwarded != null) {
                encoder.writeFrame(forwarded);
                return;
            }

            long offset = replicationManager.produceLocally(
                    r.getTopic(), r.getPartition(), r.getPayload());

            replicationManager.replicate(
                    r.getTopic(), r.getPartition(), offset, r.getPayload());

            sendProduceResponse(request, encoder, offset);
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendErrorResponse(request, encoder, e.getMessage());
        }
    }

    private void handleFetch(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {
        try {
            FetchRequest r = FetchRequest.decode(request.getPayload());

            if (!clustered()) {
                sendFetchResponse(request, encoder,
                        topicManager.fetch(r.getTopic(), r.getPartition(), r.getOffset()));
                return;
            }

            PartitionMetadata metadata =
                    clusterMetadata.getPartition(r.getTopic(), r.getPartition());

            if (metadata == null) {
                sendFetchResponse(request, encoder,
                        topicManager.fetch(r.getTopic(), r.getPartition(), r.getOffset()));
                return;
            }

            ensureLeaderAvailable(metadata);

            ProtocolFrame forwarded = forwardIfNeeded(metadata, request);
            if (forwarded != null) {
                encoder.writeFrame(forwarded);
                return;
            }

            LogRecord record = replicationManager.fetchLocally(
                    r.getTopic(), r.getPartition(), r.getOffset());

            sendFetchResponse(request, encoder,
                    record == null ? null : new FetchResult(r.getOffset(), record.getPayload()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendErrorResponse(request, encoder, e.getMessage());
        }
    }

    private void handleReplicate(ProtocolFrame request, ProtocolEncoder encoder) throws IOException {
        try {
            if (replicationManager == null) {
                throw new IllegalStateException("Replication is not enabled");
            }

            ReplicateRequest r = ReplicateRequest.decode(request.getPayload());
            replicationManager.appendReplica(
                    r.getTopic(), r.getPartition(), r.getOffset(), r.getPayload());

            byte[] payload = ByteBuffer.allocate(Long.BYTES)
                    .putLong(r.getOffset())
                    .array();

            write(request, encoder, RequestType.REPLICATE, payload);
        } catch (IllegalArgumentException | IllegalStateException e) {
            sendErrorResponse(request, encoder, e.getMessage());
        }
    }

    private boolean clustered() {
        return replicationManager != null && clusterMetadata != null;
    }

    private void ensureLeaderAvailable(PartitionMetadata metadata) {
        int leader = metadata.getLeaderBrokerId();
        if (clusterMetadata.isBrokerAlive(leader)) {
            return;
        }

        int elected = clusterMetadata.electLeader(
                metadata.getTopic(), metadata.getPartition());

        replicationManager.setLeader(
                metadata.getTopic(), metadata.getPartition(), elected);
    }

    private ProtocolFrame forwardIfNeeded(
            PartitionMetadata metadata, ProtocolFrame request) throws IOException {

        if (metadata.getLeaderBrokerId() == replicationManager.getBrokerId()) {
            return null;
        }

        int leaderId = metadata.getLeaderBrokerId();
        BrokerInfo leader = clusterMetadata.getBroker(leaderId);

        if (leader == null || leader.getPort() <= 0) {
            throw new IOException("Leader broker is not running: " + leaderId);
        }

        try {
            return BrokerClient.request(leader, request);
        } catch (IOException e) {
            clusterMetadata.markBrokerDead(leaderId);

            int elected = clusterMetadata.electLeader(
                    metadata.getTopic(), metadata.getPartition());

            replicationManager.setLeader(
                    metadata.getTopic(), metadata.getPartition(), elected);

            if (elected == replicationManager.getBrokerId()) {
                return null;
            }

            BrokerInfo newLeader = clusterMetadata.getBroker(elected);
            if (newLeader == null || newLeader.getPort() <= 0) {
                throw new IOException("Elected leader is not running: " + elected);
            }

            return BrokerClient.request(newLeader, request);
        }
    }

    private void sendProduceResponse(
            ProtocolFrame request, ProtocolEncoder encoder, long offset) throws IOException {

        byte[] payload = ByteBuffer.allocate(Long.BYTES)
                .putLong(offset)
                .array();

        write(request, encoder, RequestType.PRODUCE, payload);
    }

    private void sendFetchResponse(
            ProtocolFrame request, ProtocolEncoder encoder, FetchResult result) throws IOException {

        byte[] payload;

        if (result == null) {
            payload = ByteBuffer.allocate(Long.BYTES)
                    .putLong(-1L)
                    .array();
        } else {
            byte[] message = result.getPayload();

            payload = ByteBuffer.allocate(
                            Long.BYTES + Integer.BYTES + message.length)
                    .putLong(result.getOffset())
                    .putInt(message.length)
                    .put(message)
                    .array();
        }

        write(request, encoder, RequestType.FETCH, payload);
    }

    private void sendErrorResponse(
            ProtocolFrame request, ProtocolEncoder encoder, String message) throws IOException {

        write(request, encoder, request.getRequestType(),
                message.getBytes(StandardCharsets.UTF_8));
    }

    private void write(
            ProtocolFrame request,
            ProtocolEncoder encoder,
            RequestType type,
            byte[] payload) throws IOException {

        encoder.writeFrame(new ProtocolFrame(
                ProtocolFrame.CURRENT_VERSION,
                type,
                request.getCorrelationId(),
                payload));
    }
}
