package com.kafka.broker;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterMetadataTest {

    @Test
    void shouldStoreBrokerMetadata() {

        ClusterMetadata metadata =
                new ClusterMetadata();

        BrokerInfo broker =
                new BrokerInfo(
                        1,
                        "localhost",
                        9093);

        metadata.addBroker(broker);

        assertEquals(
                broker,
                metadata.getBroker(1));

        assertEquals(
                1,
                metadata.getBrokers().size());
    }

    @Test
    void shouldStorePartitionLeader() {

        ClusterMetadata metadata =
                new ClusterMetadata();

        PartitionMetadata partition =
                new PartitionMetadata(
                        "orders",
                        0,
                        0,
                        List.of(0, 1, 2));

        metadata.addPartition(partition);

        assertEquals(
                0,
                metadata.getLeaderBrokerId(
                        "orders",
                        0));
    }
}