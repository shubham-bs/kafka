package com.kafka.broker;

import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.ReplicateRequest;
import com.kafka.protocol.RequestType;
import com.kafka.storage.LogRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationManager implements AutoCloseable {

    private final int brokerId;
    private final Path dataDirectory;
    private final ClusterMetadata clusterMetadata;

    private final Map<String, PartitionReplica> replicas =
            new ConcurrentHashMap<>();

    private final Map<String, PartitionMetadata> metadata =
            new ConcurrentHashMap<>();

    public ReplicationManager(
            int brokerId,
            Path dataDirectory)
            throws IOException {

        this(
                brokerId,
                dataDirectory,
                new ClusterMetadata()
        );
    }

    public ReplicationManager(
            int brokerId,
            Path dataDirectory,
            ClusterMetadata clusterMetadata)
            throws IOException {

        if (brokerId < 0) {
            throw new IllegalArgumentException(
                    "Broker ID cannot be negative"
            );
        }

        if (clusterMetadata == null) {
            throw new IllegalArgumentException(
                    "Cluster metadata cannot be null"
            );
        }

        this.brokerId = brokerId;
        this.dataDirectory = dataDirectory;
        this.clusterMetadata = clusterMetadata;

        Files.createDirectories(dataDirectory);
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void createPartitionReplica(
            String topic,
            int partition,
            List<Integer> replicaBrokerIds,
            int leaderBrokerId)
            throws IOException {

        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        topic,
                        partition,
                        leaderBrokerId,
                        replicaBrokerIds
                );

        String key = key(topic, partition);

        metadata.put(key, partitionMetadata);
        clusterMetadata.addPartition(partitionMetadata);

        if (replicaBrokerIds.contains(brokerId)) {

            Path directory =
                    dataDirectory
                            .resolve(topic)
                            .resolve(
                                    "partition-" + partition
                            );

            PartitionReplica replica =
                    new PartitionReplica(
                            brokerId,
                            topic,
                            partition,
                            directory,
                            brokerId == leaderBrokerId
                    );

            PartitionReplica previous =
                    replicas.putIfAbsent(
                            key,
                            replica
                    );

            if (previous != null) {
                replica.close();
            }
        }
    }

    public PartitionMetadata getMetadata(
            String topic,
            int partition) {

        return metadata.get(
                key(topic, partition)
        );
    }

    public PartitionReplica getLocalReplica(
            String topic,
            int partition) {

        return replicas.get(
                key(topic, partition)
        );
    }

    public boolean isLeader(
            String topic,
            int partition) {

        PartitionMetadata m =
                getMetadata(topic, partition);

        return m != null
                && m.getLeaderBrokerId() == brokerId;
    }

    public long produceLocally(
            String topic,
            int partition,
            byte[] payload)
            throws IOException {

        PartitionReplica replica =
                getLocalReplica(topic, partition);

        if (replica == null) {
            throw new IllegalArgumentException(
                    "Partition replica not found"
            );
        }

        if (!replica.isLeader()) {
            throw new IllegalStateException(
                    "Broker is not partition leader"
            );
        }

        return replica.append(payload);
    }

    public LogRecord fetchLocally(
            String topic,
            int partition,
            long offset)
            throws IOException {

        PartitionReplica replica =
                getLocalReplica(topic, partition);

        if (replica == null) {
            throw new IllegalArgumentException(
                    "Partition replica not found"
            );
        }

        if (offset < 0 || offset >= replica.nextOffset()) {
            return null;
        }

        return replica.read(offset);
    }

    public void replicate(
            String topic,
            int partition,
            long offset,
            byte[] payload)
            throws IOException {

        PartitionMetadata partitionMetadata =
                metadata.get(
                        key(topic, partition)
                );

        if (partitionMetadata == null) {
            throw new IllegalArgumentException(
                    "Partition metadata not found"
            );
        }

        for (Integer followerBrokerId :
                partitionMetadata.getReplicaBrokerIds()) {

            if (followerBrokerId == brokerId) {
                continue;
            }

            BrokerInfo follower =
                    clusterMetadata.getBroker(
                            followerBrokerId
                    );

            if (follower == null) {
                continue;
            }

            if (follower.getPort() == 0) {
                continue;
            }

            ReplicateRequest replicateRequest =
                    new ReplicateRequest(
                            topic,
                            partition,
                            offset,
                            payload
                    );

            ProtocolFrame request =
                    new ProtocolFrame(
                            ProtocolFrame.CURRENT_VERSION,
                            RequestType.REPLICATE,
                            offset,
                            replicateRequest.encode()
                    );

            ProtocolFrame response =
                    BrokerClient.request(
                            follower,
                            request
                    );

            if (response.getRequestType()
                    != RequestType.REPLICATE) {

                throw new IOException(
                        "Invalid replication response from broker "
                                + followerBrokerId
                );
            }

            if (response.getPayload().length
                    != Long.BYTES) {

                throw new IOException(
                        "Invalid replication acknowledgement"
                );
            }

            long acknowledgedOffset =
                    ByteBuffer
                            .wrap(response.getPayload())
                            .getLong();

            if (acknowledgedOffset != offset) {

                throw new IOException(
                        "Follower "
                                + followerBrokerId
                                + " acknowledged offset "
                                + acknowledgedOffset
                                + " instead of "
                                + offset
                );
            }
        }
    }

    public void appendReplica(
            String topic,
            int partition,
            long offset,
            byte[] payload)
            throws IOException {

        PartitionReplica replica =
                getLocalReplica(topic, partition);

        if (replica == null) {
            throw new IllegalArgumentException(
                    "Partition replica not found"
            );
        }

        replica.appendAtOffset(
                offset,
                payload
        );
    }

    public void setLeader(
            String topic,
            int partition,
            int leaderBrokerId) {

        PartitionMetadata m =
                getMetadata(topic, partition);

        if (m == null) {
            throw new IllegalArgumentException(
                    "Partition metadata not found"
            );
        }

        m.setLeaderBrokerId(
                leaderBrokerId
        );

        PartitionReplica replica =
                getLocalReplica(
                        topic,
                        partition
                );

        if (replica != null) {
            replica.setLeader(
                    brokerId == leaderBrokerId
            );
        }
    }

    public List<String> localPartitions() {
        return new ArrayList<>(
                replicas.keySet()
        );
    }

    private String key(
            String topic,
            int partition) {

        return topic + "-" + partition;
    }

    @Override
    public void close()
            throws IOException {

        IOException firstException = null;

        for (PartitionReplica replica :
                replicas.values()) {

            try {
                replica.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }
        }

        replicas.clear();
        metadata.clear();

        if (firstException != null) {
            throw firstException;
        }
    }
}