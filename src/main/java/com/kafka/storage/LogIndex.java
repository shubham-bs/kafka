package com.kafka.storage;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
/**
 * In-memory index mapping a record's logical offset to its physical byte position inside a LogSegment.
 * The index allows for locating a record without scanning the entire log from the beginning.
 */
public class LogIndex {
    /*
     * TreeMap gives us:
     *
     * 1. O(log n) insertion
     * 2. O(log n) exact lookup
     * 3. Ordered offsets
     * 4. Ability to perform range/nearest-offset lookups later
     *
     * Using NavigableMap because the ordering capabilities
     * will become useful when implementing sequential reads.
     */
    private final NavigableMap<Long, Long> entries = new TreeMap<>();
    /**
     * Adds an offset -> file-position mapping.
     *
     * @param offset      logical record offset
     * @param filePosition byte position inside the segment
     */
    public void put(long offset, long filePosition) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        if (filePosition < 0) {
            throw new IllegalArgumentException("File position cannot be negative");
        }

        entries.put(offset, filePosition);
    }

    /**
     * Finds the exact file position for an offset.
     *
     * @return file position if the offset exists
     */
    public Optional<Long> get(long offset) {
        return Optional.ofNullable(entries.get(offset));
    }
    /**
     * Returns the number of indexed records.
     */
    public int size() {
        return entries.size();
    }
    /**
     * Returns true if the index contains the given offset.
     */
    public boolean contains(long offset) {
        return entries.containsKey(offset);
    }
    /**
     * Returns the largest indexed offset that is less than or equal to the requested offset.
     **/
    public Optional<Map.Entry<Long, Long>> floorEntry(long offset) {
        return Optional.ofNullable(entries.floorEntry(offset));
    }

    /**
     * Removes all entries.
     */
    public void clear() {
        entries.clear();
    }
}