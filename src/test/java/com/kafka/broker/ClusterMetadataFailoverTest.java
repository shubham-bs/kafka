package com.kafka.broker;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMetadataFailoverTest {

    @Test
    void shouldTrackBrokerLiveness() {

        ClusterMetadata metadata =
                new ClusterMetadata();

        metadata.addBroker(
                new BrokerInfo(
                        0,
                        "localhost",
                        9092
                )
        );

        metadata.addBroker(
                new BrokerInfo(
                        1,
                        "localhost",
                        9093
                )
        );

        assertTrue(
                metadata.isBrokerAlive(0)
        );

        assertTrue(
                metadata.isBrokerAlive(1)
        );

        metadata.markBrokerDead(0);

        assertFalse(
                metadata.isBrokerAlive(0)
        );

        metadata.markBrokerAlive(0);

        assertTrue(
                metadata.isBrokerAlive(0)
        );
    }

    @Test
    void shouldElectLowestLiveReplica() throws Exception {

        ClusterMetadata metadata =
                new ClusterMetadata();

        metadata.addBroker(
                new BrokerInfo(
                        0,
                        "localhost",
                        9092
                )
        );

        metadata.addBroker(
                new BrokerInfo(
                        1,
                        "localhost",
                        9093
                )
        );

        metadata.addBroker(
                new BrokerInfo(
                        2,
                        "localhost",
                        9094
                )
        );

        PartitionMetadata partition =
                new PartitionMetadata(
                        "orders",
                        0,
                        0,
                        List.of(0, 1, 2)
                );

        metadata.addPartition(
                partition
        );

        metadata.markBrokerDead(0);

        int leader =
                metadata.electLeader(
                        "orders",
                        0
                );

        assertEquals(
                1,
                leader
        );

        assertEquals(
                1,
                metadata.getLeaderBrokerId(
                        "orders",
                        0
                )
        );
    }

    @Test
    void shouldNotChangeLeaderWhileLeaderIsAlive() {

        ClusterMetadata metadata =
                new ClusterMetadata();

        metadata.addBroker(
                new BrokerInfo(
                        0,
                        "localhost",
                        9092
                )
        );

        metadata.addBroker(
                new BrokerInfo(
                        1,
                        "localhost",
                        9093
                )
        );

        PartitionMetadata partition =
                new PartitionMetadata(
                        "orders",
                        0,
                        0,
                        List.of(0, 1)
                );

        metadata.addPartition(
                partition
        );

        assertEquals(
                0,
                metadata.electLeader(
                        "orders",
                        0
                )
        );
    }
}