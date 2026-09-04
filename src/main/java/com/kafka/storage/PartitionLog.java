package com.kafka.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Represents the log for one partition.
 *
 * PartitionLog coordinates:
 *
 *      LogSegment
 *          +
 *      LogIndex
 *
 * It owns logical offsets while LogSegment owns
 * the physical bytes on disk.
 */
public class PartitionLog implements AutoCloseable {

    private final LogSegment segment;
    private final LogIndex index;
    private long nextOffset;
    /**
     * Opens an existing partition log or creates a new one.
     *
     * If records already exist on disk, the constructor rebuilds the in-memory state.
     */
    public PartitionLog(Path directory) throws IOException {

        Path segmentPath = directory.resolve("00000000000000000000.log");

        this.segment = new LogSegment(segmentPath);
        this.index = new LogIndex();
        recover();
    }
    /**
     * Reconstructs in-memory state from the records already stored in the segment.
     */
    private void recover() throws IOException {

        List<LogSegment.IndexedRecord> records = segment.readAll();
        if (records.isEmpty()) {
            nextOffset = 0;
            return;
        }

        for (LogSegment.IndexedRecord indexedRecord : records) {

            LogRecord record = indexedRecord.getRecord();
            index.put(
                    record.getOffset(),
                    indexedRecord.getFilePosition()
            );
        }

        LogRecord lastRecord = records.get(records.size() - 1).getRecord();
        nextOffset = lastRecord.getOffset() + 1;
    }
    /**
     * Appends a UTF-8 string to the partition.
     * @return assigned logical offset
     */
    public synchronized long append(String value) throws IOException {
        return append(
                value.getBytes(
                        StandardCharsets.UTF_8
                )
        );
    }
    /**
     * Appends raw bytes to the partition.
     *
     * Offset assignment happens here because PartitionLog owns the logical sequence.
     */
    /**
     * Appends a record and forces the segment to storage
     * before reporting success to the caller.
     *
     * @return assigned logical offset
     */
    public synchronized long append(byte[] payload) throws IOException {
        long offset = nextOffset;
        LogRecord record = new LogRecord(offset, payload);
        long filePosition = segment.append(record);
        index.put(offset, filePosition);
        segment.force();
        nextOffset++;
        return offset;
    }
    /**
     * Returns the offset that will be assigned to the next appended record.
     */
    public synchronized long nextOffset() {
        return nextOffset;
    }
    /**
     * Returns the number of indexed records.
     */
    public synchronized int size() {
        return index.size();
    }
    /**
     * Exposes the index for tests and later read-path implementation.
     */
    public synchronized LogIndex getIndex() {
        return index;
    }

    @Override
    public synchronized void close()
            throws IOException {segment.close();
    }
    /**
     * Reads a record using its logical offset.
     *
     * @return the corresponding LogRecord
     */
    public synchronized LogRecord read(long offset) throws IOException {

        long filePosition = index.get(offset).orElseThrow(() ->
                                new IllegalArgumentException("Offset not found: " + offset));

        return segment.read(filePosition);
    }
}