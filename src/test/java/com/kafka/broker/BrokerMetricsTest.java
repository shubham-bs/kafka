package com.kafka.broker;

import com.kafka.protocol.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BrokerMetricsTest {
    @Test
    void shouldTrackRequestCountsAndBytes() {
        BrokerMetrics metrics = new BrokerMetrics();
        metrics.connectionAccepted();
        metrics.request(RequestType.PRODUCE);
        metrics.recordProduce(12, 2_000_000);
        metrics.request(RequestType.FETCH);
        metrics.recordFetch(7, 4_000_000);
        metrics.request(RequestType.REPLICATE);

        BrokerMetrics.Snapshot s = metrics.snapshot();
        assertEquals(1, s.getConnectionsAccepted());
        assertEquals(3, s.getRequests());
        assertEquals(1, s.getProduces());
        assertEquals(1, s.getFetches());
        assertEquals(1, s.getReplications());
        assertEquals(12, s.getBytesProduced());
        assertEquals(7, s.getBytesFetched());
        assertEquals(2.0, s.averageProduceLatencyMillis(), 0.001);
        assertEquals(4.0, s.averageFetchLatencyMillis(), 0.001);
    }
}
