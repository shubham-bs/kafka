package com.kafka.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterMetadata {

    private final Map<Integer, BrokerInfo> brokers =
            new ConcurrentHashMap<>();

    private final Map<String, PartitionMetadata> partitions =
            new ConcurrentHashMap<>();

    private final Map<Integer, Boolean> brokerLiveness =
            new ConcurrentHashMap<>();

    public void addBroker(BrokerInfo broker) {

        if (broker == null) {
            throw new IllegalArgumentException(
                    "Broker cannot be null"
            );
        }

        brokers.put(
                broker.getBrokerId(),
                broker
        );

        brokerLiveness.putIfAbsent(
                broker.getBrokerId(),
                true
        );
    }

    public BrokerInfo getBroker(int brokerId) {

        return brokers.get(brokerId);
    }

    public List<BrokerInfo> getBrokers() {

        return Collections.unmodifiableList(
                new ArrayList<>(brokers.values())
        );
    }

    public void addPartition(
            PartitionMetadata metadata) {

        if (metadata == null) {
            throw new IllegalArgumentException(
                    "Partition metadata cannot be null"
            );
        }

        partitions.put(
                key(
                        metadata.getTopic(),
                        metadata.getPartition()
                ),
                metadata
        );
    }

    public PartitionMetadata getPartition(
            String topic,
            int partition) {

        return partitions.get(
                key(topic, partition)
        );
    }

    public List<PartitionMetadata> getPartitions() {

        return Collections.unmodifiableList(
                new ArrayList<>(partitions.values())
        );
    }

    public int getLeaderBrokerId(
            String topic,
            int partition) {

        PartitionMetadata metadata =
                getPartition(topic, partition);

        if (metadata == null) {

            throw new IllegalArgumentException(
                    "Partition metadata not found"
            );
        }

        return metadata.getLeaderBrokerId();
    }

    public void markBrokerAlive(int brokerId) {

        if (brokers.containsKey(brokerId)) {

            brokerLiveness.put(
                    brokerId,
                    true
            );
        }
    }

    public void markBrokerDead(int brokerId) {

        if (brokers.containsKey(brokerId)) {

            brokerLiveness.put(
                    brokerId,
                    false
            );
        }
    }

    public boolean isBrokerAlive(int brokerId) {

        return brokerLiveness.getOrDefault(
                brokerId,
                false
        );
    }

    public List<Integer> aliveReplicaBrokerIds(
            String topic,
            int partition) {

        PartitionMetadata metadata =
                getPartition(
                        topic,
                        partition
                );

        if (metadata == null) {

            throw new IllegalArgumentException(
                    "Partition metadata not found"
            );
        }

        List<Integer> alive =
                new ArrayList<>();

        for (Integer brokerId :
                metadata.getReplicaBrokerIds()) {

            if (isBrokerAlive(brokerId)) {
                alive.add(brokerId);
            }
        }

        return Collections.unmodifiableList(alive);
    }

    public synchronized int electLeader(
            String topic,
            int partition) {

        PartitionMetadata metadata =
                getPartition(
                        topic,
                        partition
                );

        if (metadata == null) {

            throw new IllegalArgumentException(
                    "Partition metadata not found"
            );
        }

        int currentLeader =
                metadata.getLeaderBrokerId();

        if (isBrokerAlive(currentLeader)) {

            return currentLeader;
        }

        int electedLeader =
                Integer.MAX_VALUE;

        for (Integer brokerId :
                metadata.getReplicaBrokerIds()) {

            if (isBrokerAlive(brokerId)
                    && brokerId < electedLeader) {

                electedLeader = brokerId;
            }
        }

        if (electedLeader == Integer.MAX_VALUE) {

            throw new IllegalStateException(
                    "No live replica available for "
                            + topic
                            + "-"
                            + partition
            );
        }

        metadata.setLeaderBrokerId(
                electedLeader
        );

        return electedLeader;
    }

    private String key(
            String topic,
            int partition) {

        return topic + "-" + partition;
    }
}