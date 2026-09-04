package com.kafka.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogSegmentTest {
    @Test
    void shouldAppendRecordsToDisk() throws Exception {
        Path tempDirectory = Files.createTempDirectory("kafka-log-test");
        Path segmentPath = tempDirectory.resolve("00000000000000000000.log");

        try (LogSegment segment = new LogSegment(segmentPath)) {
            LogRecord first = new LogRecord(0, "hello");
            LogRecord second = new LogRecord(1, "kafka");

            long firstPosition = segment.append(first);
            long secondPosition = segment.append(second);

            assertEquals(0, firstPosition);
            assertEquals(first.serializedSize(), secondPosition);
            assertEquals(first.serializedSize() + second.serializedSize(), segment.size());
            assertTrue(Files.exists(segmentPath));
        }
    }

    @Test
    void shouldReadExistingRecords() throws Exception {
        Path tempDirectory = Files.createTempDirectory("kafka-recovery-test");
        Path segmentPath = tempDirectory.resolve("00000000000000000000.log");
        /*
         * First broker run:
         * Write records to disk.
         */
        try (LogSegment segment = new LogSegment(segmentPath)) {
            segment.append(new LogRecord(0, "first"));
            segment.append(new LogRecord(1, "second"));
            segment.append(new LogRecord(2, "third"));;
        }
        /*
         * Simulate broker restart.
         * A completely new LogSegment object opens the existing file.
         */
        try (LogSegment recoveredSegment = new LogSegment(segmentPath)) {
            List<LogSegment.IndexedRecord> records = recoveredSegment.readAll();

            assertEquals(3, records.size());
            assertEquals(0, records.get(0)
                    .getRecord()
                    .getOffset()
            );
            assertEquals(1, records.get(1)
                            .getRecord()
                            .getOffset()
            );
            assertEquals(2, records.get(2)
                            .getRecord()
                            .getOffset()
            );
            assertEquals("first", new String(records.get(0)
                                    .getRecord()
                                    .getPayload()
                    )
            );
            assertEquals("second", new String(records.get(1)
                                    .getRecord()
                                    .getPayload()
                    )
            );
            assertEquals("third", new String(records.get(2)
                                    .getRecord()
                                    .getPayload()
                    )
            );
            /*
             * Verify physical positions are increasing.
             */
            assertEquals(0, records.get(0).getFilePosition());
            assertEquals(records.get(0)
                            .getRecord()
                            .serializedSize(),
                    records.get(1)
                            .getFilePosition()
            );
        }
    }

    @Test
    void shouldTruncateIncompleteTailRecord() throws Exception {

        Path tempDirectory = Files.createTempDirectory("partial-tail-test");
        Path segmentPath = tempDirectory.resolve("00000000000000000000.log");

        long validSize;

        try (LogSegment segment = new LogSegment(segmentPath)) {
            segment.append(new LogRecord(0, "first"));
            segment.append(new LogRecord(1, "second"));
            validSize = segment.size();
        }

        try (var channel = java.nio.channels.FileChannel.open(
                             segmentPath,
                             java.nio.file.StandardOpenOption.WRITE,
                             java.nio.file.StandardOpenOption.APPEND
                     )) {

            java.nio.ByteBuffer partialRecord = java.nio.ByteBuffer.allocate(8);


            partialRecord.putInt(100);
            partialRecord.putInt(12345);
            partialRecord.flip();

            channel.write(partialRecord);
        }

        assertTrue(Files.size(segmentPath) > validSize);

        try (LogSegment recoveredSegment = new LogSegment(segmentPath)) {

            List<LogSegment.IndexedRecord> records = recoveredSegment.readAll();

            assertEquals(2, records.size());
            assertEquals(0, records.get(0)
                            .getRecord()
                            .getOffset());

            assertEquals(1, records.get(1)
                            .getRecord()
                            .getOffset());
            assertEquals(validSize, Files.size(segmentPath));
            assertEquals(validSize, recoveredSegment.size());
        }
    }
}