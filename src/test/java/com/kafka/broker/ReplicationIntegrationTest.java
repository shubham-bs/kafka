package com.kafka.broker;

import com.kafka.protocol.FetchRequest;
import com.kafka.protocol.ProduceRequest;
import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;
import com.kafka.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldReplicateAcrossThreeLiveBrokers()
            throws Exception {

        KafkaBroker broker0 =
                createBroker(0);

        KafkaBroker broker1 =
                createBroker(1);

        KafkaBroker broker2 =
                createBroker(2);

        Thread broker0Thread =
                startBroker(broker0);

        Thread broker1Thread =
                startBroker(broker1);

        Thread broker2Thread =
                startBroker(broker2);

        try {

            waitForBroker(broker0);
            waitForBroker(broker1);
            waitForBroker(broker2);

            registerCluster(
                    broker0,
                    broker1,
                    broker2
            );

            configurePartition(broker0);
            configurePartition(broker1);
            configurePartition(broker2);

            /*
             * -------------------------------------------------
             * TEST 1
             *
             * Direct produce to leader.
             * Broker 0 should replicate to 1 and 2.
             * -------------------------------------------------
             */

            byte[] firstMessage =
                    "message-from-leader"
                            .getBytes(StandardCharsets.UTF_8);

            long firstOffset =
                    produce(
                            broker0.getPort(),
                            "orders",
                            0,
                            firstMessage
                    );

            assertEquals(0, firstOffset);

            waitForReplica(
                    broker1,
                    firstMessage,
                    0
            );

            waitForReplica(
                    broker2,
                    firstMessage,
                    0
            );

            /*
             * Verify follower 1.
             */
            LogRecord broker1Record =
                    broker1
                            .getReplicationManager()
                            .fetchLocally(
                                    "orders",
                                    0,
                                    0
                            );

            assertNotNull(broker1Record);

            assertArrayEquals(
                    firstMessage,
                    broker1Record.getPayload()
            );

            /*
             * Verify follower 2.
             */
            LogRecord broker2Record =
                    broker2
                            .getReplicationManager()
                            .fetchLocally(
                                    "orders",
                                    0,
                                    0
                            );

            assertNotNull(broker2Record);

            assertArrayEquals(
                    firstMessage,
                    broker2Record.getPayload()
            );

            /*
             * -------------------------------------------------
             * TEST 2
             *
             * Produce through follower Broker 1.
             *
             * Broker 1 must forward PRODUCE to Broker 0.
             * Broker 0 produces and replicates the record.
             * -------------------------------------------------
             */

            byte[] secondMessage =
                    "message-through-follower"
                            .getBytes(StandardCharsets.UTF_8);

            long secondOffset =
                    produce(
                            broker1.getPort(),
                            "orders",
                            0,
                            secondMessage
                    );

            assertEquals(1, secondOffset);

            waitForReplica(
                    broker1,
                    secondMessage,
                    1
            );

            waitForReplica(
                    broker2,
                    secondMessage,
                    1
            );

            /*
             * Leader should contain the second record.
             */
            LogRecord leaderRecord =
                    broker0
                            .getReplicationManager()
                            .fetchLocally(
                                    "orders",
                                    0,
                                    1
                            );

            assertNotNull(leaderRecord);

            assertArrayEquals(
                    secondMessage,
                    leaderRecord.getPayload()
            );

            /*
             * -------------------------------------------------
             * TEST 3
             *
             * Fetch through follower Broker 2.
             *
             * Broker 2 must forward FETCH to Broker 0.
             * -------------------------------------------------
             */

            LogRecord fetched =
                    fetch(
                            broker2.getPort(),
                            "orders",
                            0,
                            1
                    );

            assertNotNull(fetched);

            assertEquals(
                    1,
                    fetched.getOffset()
            );

            assertArrayEquals(
                    secondMessage,
                    fetched.getPayload()
            );

        } finally {

            broker0.shutdown();
            broker1.shutdown();
            broker2.shutdown();

            broker0Thread.join(2000);
            broker1Thread.join(2000);
            broker2Thread.join(2000);
        }
    }

    private KafkaBroker createBroker(
            int brokerId)
            throws Exception {

        Path dataDirectory =
                tempDir.resolve(
                        "broker-" + brokerId
                );

        Files.createDirectories(
                dataDirectory
        );

        return new KafkaBroker(
                brokerId,
                0,
                4,
                dataDirectory
        );
    }

    private void registerCluster(
            KafkaBroker broker0,
            KafkaBroker broker1,
            KafkaBroker broker2) {

        BrokerInfo info0 =
                new BrokerInfo(
                        0,
                        "127.0.0.1",
                        broker0.getPort()
                );

        BrokerInfo info1 =
                new BrokerInfo(
                        1,
                        "127.0.0.1",
                        broker1.getPort()
                );

        BrokerInfo info2 =
                new BrokerInfo(
                        2,
                        "127.0.0.1",
                        broker2.getPort()
                );

        registerBroker(
                broker0,
                info0,
                info1,
                info2
        );

        registerBroker(
                broker1,
                info0,
                info1,
                info2
        );

        registerBroker(
                broker2,
                info0,
                info1,
                info2
        );
    }

    private void registerBroker(
            KafkaBroker broker,
            BrokerInfo... brokers) {

        for (BrokerInfo info : brokers) {

            broker.getClusterMetadata()
                    .addBroker(info);
        }
    }

    private void configurePartition(
            KafkaBroker broker)
            throws Exception {

        broker.getReplicationManager()
                .createPartitionReplica(
                        "orders",
                        0,
                        List.of(0, 1, 2),
                        0
                );
    }

    private Thread startBroker(
            KafkaBroker broker) {

        Thread thread =
                new Thread(() -> {

                    try {
                        broker.start();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        thread.start();

        return thread;
    }

    private void waitForBroker(
            KafkaBroker broker)
            throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + 5000;

        while (broker.getPort() <= 0) {

            if (System.currentTimeMillis()
                    > deadline) {

                fail(
                        "Broker "
                                + broker.getBrokerId()
                                + " failed to start"
                );
            }

            Thread.sleep(10);
        }
    }

    private long produce(
            int port,
            String topic,
            int partition,
            byte[] payload)
            throws Exception {

        ProduceRequest requestPayload =
                new ProduceRequest(
                        topic,
                        partition,
                        payload
                );

        ProtocolFrame request =
                new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        RequestType.PRODUCE,
                        1,
                        requestPayload.encode()
                );

        ProtocolFrame response =
                send(
                        port,
                        request
                );

        assertEquals(
                RequestType.PRODUCE,
                response.getRequestType()
        );

        assertEquals(
                Long.BYTES,
                response.getPayload().length
        );

        return ByteBuffer
                .wrap(response.getPayload())
                .getLong();
    }

    private LogRecord fetch(
            int port,
            String topic,
            int partition,
            long offset)
            throws Exception {

        FetchRequest requestPayload =
                new FetchRequest(
                        topic,
                        partition,
                        offset
                );

        ProtocolFrame request =
                new ProtocolFrame(
                        ProtocolFrame.CURRENT_VERSION,
                        RequestType.FETCH,
                        2,
                        requestPayload.encode()
                );

        ProtocolFrame response =
                send(
                        port,
                        request
                );

        assertEquals(
                RequestType.FETCH,
                response.getRequestType()
        );

        byte[] payload =
                response.getPayload();

        if (payload.length == Long.BYTES) {
            long responseOffset =
                    ByteBuffer
                            .wrap(payload)
                            .getLong();

            if (responseOffset == -1) {
                return null;
            }
        }

        ByteBuffer buffer =
                ByteBuffer.wrap(payload);

        long responseOffset =
                buffer.getLong();

        int messageLength =
                buffer.getInt();

        byte[] message =
                new byte[messageLength];

        buffer.get(message);

        return new LogRecord(
                responseOffset,
                message
        );
    }

    private void waitForReplica(
            KafkaBroker broker,
            byte[] expected,
            long offset)
            throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + 5000;

        while (System.currentTimeMillis()
                < deadline) {

            LogRecord record =
                    broker
                            .getReplicationManager()
                            .fetchLocally(
                                    "orders",
                                    0,
                                    offset
                            );

            if (record != null
                    && java.util.Arrays.equals(
                    expected,
                    record.getPayload()
            )) {

                return;
            }

            Thread.sleep(20);
        }

        fail(
                "Broker "
                        + broker.getBrokerId()
                        + " did not receive replica"
        );
    }

    private ProtocolFrame send(
            int port,
            ProtocolFrame request)
            throws Exception {

        try (Socket socket =
                     new Socket(
                             "127.0.0.1",
                             port
                     )) {

            socket.setSoTimeout(5000);

            DataOutputStream output =
                    new DataOutputStream(
                            socket.getOutputStream()
                    );

            DataInputStream input =
                    new DataInputStream(
                            socket.getInputStream()
                    );

            ProtocolEncoder encoder =
                    new ProtocolEncoder(output);

            ProtocolDecoder decoder =
                    new ProtocolDecoder(input);

            encoder.writeFrame(request);

            return decoder.readFrame();
        }
    }
}