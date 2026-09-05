package com.kafka.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PartitionMetadata {

    private final String topic;
    private final int partition;
    private volatile int leaderBrokerId;
    private final List<Integer> replicaBrokerIds;

    public PartitionMetadata(
            String topic,
            int partition,
            int leaderBrokerId,
            List<Integer> replicaBrokerIds) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative");
        }

        if (replicaBrokerIds == null || replicaBrokerIds.isEmpty()) {
            throw new IllegalArgumentException("Replica list cannot be empty");
        }

        if (!replicaBrokerIds.contains(leaderBrokerId)) {
            throw new IllegalArgumentException("Leader must be a replica");
        }

        this.topic = topic;
        this.partition = partition;
        this.leaderBrokerId = leaderBrokerId;
        this.replicaBrokerIds = Collections.unmodifiableList(new ArrayList<>(replicaBrokerIds));
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public int getLeaderBrokerId() {
        return leaderBrokerId;
    }

    public void setLeaderBrokerId(int leaderBrokerId) {

        if (!replicaBrokerIds.contains(leaderBrokerId)) {
            throw new IllegalArgumentException("Leader must be a replica");
        }

        this.leaderBrokerId = leaderBrokerId;
    }

    public List<Integer> getReplicaBrokerIds() {
        return replicaBrokerIds;
    }

    @Override
    public String toString() {
        return "PartitionMetadata{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", leaderBrokerId=" + leaderBrokerId +
                ", replicaBrokerIds=" + replicaBrokerIds +
                '}';
    }
}