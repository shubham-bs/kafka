package com.kafka.broker;

import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class FailureDetector implements AutoCloseable {

    private final int localBrokerId;
    private final ClusterMetadata clusterMetadata;
    private final ReplicationManager replicationManager;
    private final long intervalMillis;

    private final ScheduledExecutorService scheduler;

    public FailureDetector(
            int localBrokerId,
            ClusterMetadata clusterMetadata,
            ReplicationManager replicationManager,
            long intervalMillis) {

        if (localBrokerId < 0) {
            throw new IllegalArgumentException(
                    "Broker ID cannot be negative"
            );
        }

        if (clusterMetadata == null) {
            throw new IllegalArgumentException(
                    "Cluster metadata cannot be null"
            );
        }

        if (replicationManager == null) {
            throw new IllegalArgumentException(
                    "Replication manager cannot be null"
            );
        }

        if (intervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "Failure detection interval must be positive"
            );
        }

        this.localBrokerId = localBrokerId;
        this.clusterMetadata = clusterMetadata;
        this.replicationManager = replicationManager;
        this.intervalMillis = intervalMillis;

        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "broker-failure-detector-"
                                                    + localBrokerId
                                    );

                            thread.setDaemon(true);

                            return thread;
                        }
                );
    }

    public void start() {

        scheduler.scheduleWithFixedDelay(
                this::checkNow,
                0,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    public void checkNow() {

        probeBrokers();

        electLeaders();
    }

    private void probeBrokers() {

        List<BrokerInfo> brokers =
                clusterMetadata.getBrokers();

        for (BrokerInfo broker : brokers) {

            if (broker.getBrokerId() == localBrokerId) {

                clusterMetadata.markBrokerAlive(
                        localBrokerId
                );

                continue;
            }

            if (broker.getPort() <= 0) {

                clusterMetadata.markBrokerDead(
                        broker.getBrokerId()
                );

                continue;
            }

            try {

                ProtocolFrame request =
                        new ProtocolFrame(
                                ProtocolFrame.CURRENT_VERSION,
                                RequestType.PING,
                                System.nanoTime(),
                                new byte[0]
                        );

                ProtocolFrame response =
                        BrokerClient.request(
                                broker,
                                request
                        );

                boolean alive =
                        response.getRequestType()
                                == RequestType.PING
                                && "PONG".equals(
                                new String(
                                        response.getPayload(),
                                        StandardCharsets.UTF_8
                                )
                        );

                if (alive) {

                    clusterMetadata.markBrokerAlive(
                            broker.getBrokerId()
                    );

                } else {

                    clusterMetadata.markBrokerDead(
                            broker.getBrokerId()
                    );
                }

            } catch (IOException | RuntimeException e) {

                clusterMetadata.markBrokerDead(
                        broker.getBrokerId()
                );
            }
        }
    }

    private void electLeaders() {

        List<PartitionMetadata> partitions =
                clusterMetadata.getPartitions();

        for (PartitionMetadata partition : partitions) {

            int currentLeader =
                    partition.getLeaderBrokerId();

            if (clusterMetadata.isBrokerAlive(
                    currentLeader
            )) {
                continue;
            }

            try {

                int electedLeader =
                        clusterMetadata.electLeader(
                                partition.getTopic(),
                                partition.getPartition()
                        );

                replicationManager.setLeader(
                        partition.getTopic(),
                        partition.getPartition(),
                        electedLeader
                );

            } catch (IllegalStateException e) {

                // No live replica is currently available.
            }
        }
    }

    @Override
    public void close() {

        scheduler.shutdownNow();
    }
}