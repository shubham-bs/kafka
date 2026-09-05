package com.kafka.broker;

import com.kafka.protocol.*;
import com.kafka.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationIntegrationTest {
    @TempDir Path tempDir;

    @Test
    void shouldReplicateAcrossThreeLiveBrokers() throws Exception {
        KafkaBroker[] b = brokers();
        try {
            startAndRegister(b);
            configure(b);
            byte[] first = bytes("message-from-leader");
            assertEquals(0, produce(b[0].getPort(), first));
            waitForReplica(b[1], first, 0);
            waitForReplica(b[2], first, 0);
            assertRecord(b[1], first, 0);
            assertRecord(b[2], first, 0);

            byte[] second = bytes("message-through-follower");
            assertEquals(1, produce(b[1].getPort(), second));
            waitForReplica(b[1], second, 1);
            waitForReplica(b[2], second, 1);
            assertRecord(b[0], second, 1);
            assertRecord(fetch(b[2].getPort(), 1), second, 1);
        } finally { shutdown(b); }
    }

    @Test
    void shouldFailoverToBrokerOneAfterLeaderFailure() throws Exception {
        KafkaBroker[] b = brokers();
        try {
            startAndRegister(b);
            configure(b);
            byte[] first = bytes("before-failure");
            assertEquals(0, produce(b[0].getPort(), first));
            waitForReplica(b[1], first, 0);
            waitForReplica(b[2], first, 0);

            b[0].shutdown();
            waitForLeaderElection(b[1], 1);
            assertTrue(b[1].getReplicationManager().getLocalReplica("orders", 0).isLeader());

            byte[] second = bytes("after-failure");
            assertEquals(1, produce(b[1].getPort(), second));
            waitForReplica(b[2], second, 1);
            assertRecord(b[1], second, 1);
            assertRecord(b[2], second, 1);
            assertRecord(fetch(b[1].getPort(), 1), second, 1);
        } finally { shutdown(b); }
    }

    private KafkaBroker[] brokers() throws Exception {
        KafkaBroker[] result = new KafkaBroker[3];
        for (int i = 0; i < result.length; i++) {
            Path dir = tempDir.resolve("broker-" + i);
            Files.createDirectories(dir);
            result[i] = new KafkaBroker(i, 0, 4, dir);
        }
        return result;
    }

    private void startAndRegister(KafkaBroker[] b) throws Exception {
        Thread[] threads = new Thread[b.length];
        for (int i = 0; i < b.length; i++) threads[i] = startBroker(b[i]);
        for (KafkaBroker broker : b) waitForBroker(broker);
        BrokerInfo[] info = new BrokerInfo[b.length];
        for (int i = 0; i < b.length; i++) info[i] = new BrokerInfo(i, "127.0.0.1", b[i].getPort());
        for (KafkaBroker broker : b) for (BrokerInfo x : info) broker.getClusterMetadata().addBroker(x);
    }

    private void configure(KafkaBroker[] b) throws Exception {
        for (KafkaBroker broker : b)
            broker.getReplicationManager().createPartitionReplica("orders", 0, List.of(0, 1, 2), 0);
    }

    private Thread startBroker(KafkaBroker broker) {
        Thread t = new Thread(() -> {
            try { broker.start(); } catch (Exception e) { throw new RuntimeException(e); }
        });
        t.start();
        return t;
    }

    private void waitForBroker(KafkaBroker broker) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (broker.getPort() <= 0 && System.currentTimeMillis() < deadline) Thread.sleep(10);
        if (broker.getPort() <= 0) fail("Broker " + broker.getBrokerId() + " failed to start");
    }

    private void waitForLeaderElection(KafkaBroker broker, int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.getClusterMetadata().getLeaderBrokerId("orders", 0) == expected) return;
            Thread.sleep(20);
        }
        fail("Broker " + broker.getBrokerId() + " did not elect broker " + expected + " as leader");
    }

    private long produce(int port, byte[] payload) throws Exception {
        ProduceRequest body = new ProduceRequest("orders", 0, payload);
        ProtocolFrame request = frame(RequestType.PRODUCE, 1, body.encode());
        ProtocolFrame response = send(port, request);
        assertEquals(RequestType.PRODUCE, response.getRequestType());
        assertEquals(Long.BYTES, response.getPayload().length);
        return ByteBuffer.wrap(response.getPayload()).getLong();
    }

    private LogRecord fetch(int port, long offset) throws Exception {
        FetchRequest body = new FetchRequest("orders", 0, offset);
        ProtocolFrame response = send(port, frame(RequestType.FETCH, 2, body.encode()));
        assertEquals(RequestType.FETCH, response.getRequestType());
        ByteBuffer buffer = ByteBuffer.wrap(response.getPayload());
        long responseOffset = buffer.getLong();
        if (responseOffset == -1) return null;
        int length = buffer.getInt();
        byte[] message = new byte[length];
        buffer.get(message);
        return new LogRecord(responseOffset, message);
    }

    private ProtocolFrame frame(RequestType type, int correlation, byte[] payload) {
        return new ProtocolFrame(ProtocolFrame.CURRENT_VERSION, type, correlation, payload);
    }

    private ProtocolFrame send(int port, ProtocolFrame request) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            socket.setSoTimeout(5000);
            new ProtocolEncoder(out).writeFrame(request);
            return new ProtocolDecoder(in).readFrame();
        }
    }

    private void waitForReplica(KafkaBroker broker, byte[] expected, long offset) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            LogRecord record = broker.getReplicationManager().fetchLocally("orders", 0, offset);
            if (record != null && Arrays.equals(expected, record.getPayload())) return;
            Thread.sleep(20);
        }
        fail("Broker " + broker.getBrokerId() + " did not receive replica");
    }

    private void assertRecord(KafkaBroker broker, byte[] expected, long offset) throws Exception {
        assertRecord(broker.getReplicationManager().fetchLocally("orders", 0, offset), expected, offset);
    }

    private void assertRecord(LogRecord record, byte[] expected, long offset) {
        assertNotNull(record);
        assertEquals(offset, record.getOffset());
        assertArrayEquals(expected, record.getPayload());
    }

    private byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private void shutdown(KafkaBroker[] b) throws InterruptedException {
        for (KafkaBroker broker : b) broker.shutdown();
    }
}
