package com.kafka.broker;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FailureDetectorTest {

    @Test
    void shouldElectLiveReplicaAfterLeaderDies()
            throws Exception {

        Path directory =
                Files.createTempDirectory(
                        "failure-detector-test"
                );

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

        ReplicationManager replicationManager =
                new ReplicationManager(
                        1,
                        directory,
                        metadata
                );

        replicationManager.createPartitionReplica(
                "orders",
                0,
                List.of(0, 1, 2),
                0
        );

        metadata.markBrokerDead(0);

        try (
                FailureDetector detector =
                        new FailureDetector(
                                1,
                                metadata,
                                replicationManager,
                                1000
                        )
        ) {

            detector.checkNow();

            assertEquals(
                    1,
                    metadata.getLeaderBrokerId(
                            "orders",
                            0
                    )
            );

            assertEquals(
                    1,
                    replicationManager
                            .getLocalReplica(
                                    "orders",
                                    0
                            )
                            .isLeader()
                            ? 1
                            : 0
            );
        }

        replicationManager.close();
    }
}