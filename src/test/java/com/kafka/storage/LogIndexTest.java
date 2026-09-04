package com.kafka.storage;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LogIndexTest {

    @Test
    void shouldStoreAndRetrieveOffset() {
        LogIndex index = new LogIndex();

        index.put(0, 0);
        index.put(1, 25);
        index.put(2, 50);


        assertEquals(Optional.of(0L), index.get(0));
        assertEquals(Optional.of(25L), index.get(1));
        assertEquals(Optional.of(50L), index.get(2));
    }


    @Test
    void shouldReturnEmptyForUnknownOffset() {
        LogIndex index = new LogIndex();

        index.put(0, 0);

        assertTrue(index.get(99).isEmpty());
    }


    @Test
    void shouldFindNearestLowerOffset() {

        LogIndex index = new LogIndex();

        index.put(0, 0);
        index.put(10, 100);
        index.put(20, 200);


        Optional<Map.Entry<Long, Long>> result = index.floorEntry(17);

        assertTrue(result.isPresent());

        assertEquals(10L, result.get().getKey());

        assertEquals(100L, result.get().getValue());
    }
}