package com.kafka.broker;

import com.kafka.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Topic implements AutoCloseable {

    private final String name;
    private final List<PartitionLog> partitions;

    public Topic(String name, int partitionCount, Path topicDirectory) throws IOException {

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }

        this.name = name;
        this.partitions = new ArrayList<>(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            Path partitionDirectory = topicDirectory.resolve("partition-" + i);

            partitions.add(new PartitionLog(partitionDirectory));
        }
    }

    public String getName() {
        return name;
    }

    public int partitionCount() {
        return partitions.size();
    }

    public PartitionLog getPartition(int partition) throws IOException {

        if (partition < 0 || partition >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }

        return partitions.get(partition);
    }

    @Override
    public void close() throws IOException {
        IOException firstException = null;

        for (PartitionLog partition : partitions) {
            try {
                partition.close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
            }
        }

        if (firstException != null) throw firstException;
    }
}