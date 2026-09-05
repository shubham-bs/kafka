package com.kafka.broker;

import com.kafka.storage.LogRecord;
import com.kafka.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Path;

public class PartitionReplica {

    private final int brokerId;
    private final String topic;
    private final int partition;
    private final PartitionLog log;

    private volatile boolean leader;

    public PartitionReplica(
            int brokerId,
            String topic,
            int partition,
            Path directory,
            boolean leader) throws IOException{

        this.brokerId = brokerId;
        this.topic = topic;
        this.partition = partition;
        this.log = new PartitionLog(directory);
        this.leader = leader;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public boolean isLeader() {
        return leader;
    }

    public void setLeader(boolean leader) {
        this.leader = leader;
    }

    public synchronized long append(byte[] payload) throws IOException {
        return log.append(payload);
    }

    public synchronized void appendAtOffset(
            long expectedOffset,
            byte[] payload) throws IOException {

        if (log.nextOffset() != expectedOffset) {
            if (log.nextOffset() > expectedOffset) {
                LogRecord existing = log.read(expectedOffset);

                if (!java.util.Arrays.equals(existing.getPayload(), payload)) {

                    throw new IOException(
                            "Replica offset conflict at " + expectedOffset);
                }

                return;
            }

            throw new IOException(
                    "Replica offset gap. Expected "
                            + log.nextOffset()
                            + " but received "
                            + expectedOffset);
        }

        long actualOffset = log.append(payload);

        if (actualOffset != expectedOffset) {
            throw new IOException(
                    "Replica offset mismatch. Expected "
                            + expectedOffset
                            + " but wrote "
                            + actualOffset);
        }
    }

    public synchronized LogRecord read(long offset) throws IOException {

        return log.read(offset);
    }

    public synchronized long nextOffset() {
        return log.nextOffset();
    }

    public synchronized int size() {
        return log.size();
    }

    public synchronized void close() throws IOException {
        log.close();
    }
}