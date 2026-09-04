package com.kafka.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents one append-only segment of a partition log.
 *
 * A segment is backed by a single file.
 *
 * Responsibilities:
 *
 *  1. Open/create the segment file.
 *  2. Append LogRecord objects sequentially.
 *  3. Read existing records sequentially.
 *  4. Track the current write position.
 *  5. Force buffered data to storage.
 *  6. Close the file.
 *
 * Offset indexing and higher-level recovery decisions belong
 * to PartitionLog.
 */
public class LogSegment implements AutoCloseable {

    private final Path filePath;
    private final FileChannel fileChannel;

    /*
     * Position immediately after the last valid record.
     * New records are appended from this position.
     */
    private long writePosition;
    /**
     * Opens an existing segment or creates a new one.
     */
    public LogSegment(Path filePath) throws IOException {
        this.filePath = filePath;

        Path parent = filePath.getParent();

        if (parent != null) Files.createDirectories(parent);

        this.fileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        /*
         * If this is an existing segment, start writing at the end rather than overwriting existing records.
         */
        this.writePosition = fileChannel.size();
    }

    /**
     * Appends one record to the end of the segment.
     *
     * @return byte position where the record begins
     */
    public synchronized long append(LogRecord record) throws IOException {
        long recordPosition = writePosition;
        byte[] serialized = record.serialize();

        ByteBuffer buffer = ByteBuffer.wrap(serialized);

        /*
         * FileChannel.write() is not guaranteed to consume the entire buffer in one call.
         * Keep writing until every byte has been consumed.
         */
        while (buffer.hasRemaining()) {
            int bytesWritten = fileChannel.write(buffer, writePosition);
            writePosition += bytesWritten;
        }
        return recordPosition;
    }

    /**
     * Reads every complete record currently present in the segment.
     * Records are read sequentially from byte position 0.
     */
    /**
     * Reads all valid records from the segment.
     *
     * If the final record is incomplete, the incomplete tail
     * is truncated because it may have been caused by a crash
     * during an append.
     *
     * Corruption in the middle of the segment is treated as
     * a hard error.
     */
    public synchronized List<IndexedRecord> readAll() throws IOException {

        List<IndexedRecord> records = new ArrayList<>();

        long position = 0;
        long fileSize = fileChannel.size();

        while (position < fileSize) {
            if (fileSize - position < Integer.BYTES) {
                fileChannel.truncate(position);
                writePosition = position;
                break;
            }

            ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);

            readFully(lengthBuffer, position);

            lengthBuffer.flip();

            int recordBodyLength = lengthBuffer.getInt();

            if (recordBodyLength < Long.BYTES + Integer.BYTES) {

                throw new IOException("Corrupt record at position "
                                + position
                                + ": invalid length "
                                + recordBodyLength);
            }

            long recordEnd = position + Integer.BYTES + recordBodyLength;

            if (recordEnd > fileSize) {
                fileChannel.truncate(position);
                writePosition = position;
                break;
            }

            ByteBuffer recordBuffer = ByteBuffer.allocate(Integer.BYTES + recordBodyLength);

            readFully(recordBuffer, position);

            recordBuffer.flip();

            byte[] serialized = new byte[recordBuffer.remaining()];

            recordBuffer.get(serialized);

            LogRecord record = LogRecord.deserialize(serialized);

            records.add(new IndexedRecord(record, position));

            position = recordEnd;
        }
        return records;
    }

    /**
     * Reads exactly enough bytes to fill the supplied buffer.
     *
     * A single FileChannel.read() is NOT guaranteed to return all requested bytes.
     */
    private void readFully(ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = fileChannel.read(buffer, position);
            if (bytesRead == -1) {
                throw new EOFException("Unexpected end of segment");
            }
            position += bytesRead;
        }
    }
    /**
     * Forces buffered file data to storage.
     */
    public synchronized void force() throws IOException {
        fileChannel.force(false);
    }
    /**
     * Returns the current segment size.
     */
    public synchronized long size() {
        return writePosition;
    }

    public Path getFilePath() {
        return filePath;
    }

    @Override
    public synchronized void close() throws IOException {
        fileChannel.close();
    }

    /**
     * Result of reading one record from the segment.
     *
     * Contains both:
     *  - the logical LogRecord
     *  - its physical byte position
     *
     * PartitionLog will use the position to rebuild the offset -> file position index.
     */
    public static class IndexedRecord {

        private final LogRecord record;
        private final long filePosition;

        public IndexedRecord(LogRecord record, long filePosition) {
            this.record = record;
            this.filePosition = filePosition;
        }

        public LogRecord getRecord() {
            return record;
        }

        public long getFilePosition() {
            return filePosition;
        }
    }

    /**
     * Reads one record from a known physical file position.
     *
     * @param position byte position where the record begins
     * @return decoded LogRecord
     */
    public synchronized LogRecord read(long position) throws IOException {

        if (position < 0 || position >= writePosition) {
            throw new IllegalArgumentException("Invalid record position: " + position);
        }
        /*
         * First read the 4-byte record length.
         */
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        readFully(lengthBuffer, position);
        lengthBuffer.flip();
        int recordBodyLength = lengthBuffer.getInt();

        if (recordBodyLength < Long.BYTES + Integer.BYTES) {
            throw new IOException("Invalid record length: " + recordBodyLength);
        }

        long recordEnd = position + Integer.BYTES + recordBodyLength;

        if (recordEnd > writePosition) {
            throw new EOFException("Incomplete record at position: " + position);
        }
        /*
         * Read the complete serialized record.
         */
        ByteBuffer recordBuffer = ByteBuffer.allocate(Integer.BYTES + recordBodyLength);

        readFully(recordBuffer, position);

        recordBuffer.flip();

        byte[] serialized = new byte[recordBuffer.remaining()];

        recordBuffer.get(serialized);

        return LogRecord.deserialize(serialized);
    }
}