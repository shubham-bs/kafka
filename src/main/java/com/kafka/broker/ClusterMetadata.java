package com.kafka.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterMetadata {

    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();

    private final Map<String, PartitionMetadata> partitions = new ConcurrentHashMap<>();

    public void addBroker(BrokerInfo broker) {
        brokers.put(broker.getBrokerId(), broker);
    }

    public BrokerInfo getBroker(int brokerId) {
        return brokers.get(brokerId);
    }

    public List<BrokerInfo> getBrokers() {
        return Collections.unmodifiableList(new ArrayList<>(brokers.values()));
    }

    public void addPartition(PartitionMetadata metadata) {

        partitions.put(
                key(
                        metadata.getTopic(),
                        metadata.getPartition()),
                metadata);
    }

    public PartitionMetadata getPartition(String topic, int partition) {
        return partitions.get(key(topic, partition));
    }

    public int getLeaderBrokerId(String topic, int partition) {

        PartitionMetadata metadata = getPartition(topic, partition);

        if (metadata == null) {
            throw new IllegalArgumentException("Partition metadata not found");
        }

        return metadata.getLeaderBrokerId();
    }

    private String key(String topic, int partition) {
        return topic + "-" + partition;
    }
}