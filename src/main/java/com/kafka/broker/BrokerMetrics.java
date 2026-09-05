package com.kafka.broker;

import com.kafka.protocol.RequestType;

import java.util.concurrent.atomic.AtomicLong;

public final class BrokerMetrics {
    private final AtomicLong connectionsAccepted = new AtomicLong();
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong produces = new AtomicLong();
    private final AtomicLong fetches = new AtomicLong();
    private final AtomicLong replications = new AtomicLong();
    private final AtomicLong bytesProduced = new AtomicLong();
    private final AtomicLong bytesFetched = new AtomicLong();
    private final AtomicLong produceLatencyNanos = new AtomicLong();
    private final AtomicLong produceSamples = new AtomicLong();
    private final AtomicLong fetchLatencyNanos = new AtomicLong();
    private final AtomicLong fetchSamples = new AtomicLong();

    public void connectionAccepted() { connectionsAccepted.incrementAndGet(); }

    public void request(RequestType type) {
        requests.incrementAndGet();
        if (type == RequestType.PRODUCE) {
            produces.incrementAndGet();
        } else if (type == RequestType.FETCH) {
            fetches.incrementAndGet();
        } else if (type == RequestType.REPLICATE) {
            replications.incrementAndGet();
        }
    }

    public void recordFetch(int bytes, long latencyNanos) {
        bytesFetched.addAndGet(bytes);
        fetchLatencyNanos.addAndGet(latencyNanos);
        fetchSamples.incrementAndGet();
    }

    public void recordProduce(int payloadBytes, long latencyNanos) {
        bytesProduced.addAndGet(payloadBytes);
        produceLatencyNanos.addAndGet(latencyNanos);
        produceSamples.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                connectionsAccepted.get(),
                requests.get(),
                produces.get(),
                fetches.get(),
                replications.get(),
                bytesProduced.get(),
                bytesFetched.get(),
                produceLatencyNanos.get(),
                fetchLatencyNanos.get(),
                produceSamples.get(),
                fetchSamples.get());
    }

    public static final class Snapshot {
        private final long connectionsAccepted, requests, produces, fetches, replications;
        private final long bytesProduced, bytesFetched, produceLatencyNanos, fetchLatencyNanos, produceSamples, fetchSamples;

        private Snapshot(long connectionsAccepted, long requests, long produces,
                         long fetches, long replications, long bytesProduced,
                         long bytesFetched, long produceLatencyNanos,
                         long fetchLatencyNanos, long produceSamples, long fetchSamples) {
            this.connectionsAccepted = connectionsAccepted;
            this.requests = requests;
            this.produces = produces;
            this.fetches = fetches;
            this.replications = replications;
            this.bytesProduced = bytesProduced;
            this.bytesFetched = bytesFetched;
            this.produceLatencyNanos = produceLatencyNanos;
            this.fetchLatencyNanos = fetchLatencyNanos;
            this.produceSamples = produceSamples;
            this.fetchSamples = fetchSamples;
        }

        public long getConnectionsAccepted() { return connectionsAccepted; }
        public long getRequests() { return requests; }
        public long getProduces() { return produces; }
        public long getFetches() { return fetches; }
        public long getReplications() { return replications; }
        public long getBytesProduced() { return bytesProduced; }
        public long getBytesFetched() { return bytesFetched; }

        public double averageProduceLatencyMillis() {
            return produceSamples == 0 ? 0.0 : produceLatencyNanos / 1_000_000.0 / produceSamples;
        }

        public double averageFetchLatencyMillis() {
            return fetchSamples == 0 ? 0.0 : fetchLatencyNanos / 1_000_000.0 / fetchSamples;
        }

        @Override
        public String toString() {
            return "BrokerMetrics{" +
                    "connections=" + connectionsAccepted +
                    ", requests=" + requests +
                    ", produces=" + produces +
                    ", fetches=" + fetches +
                    ", replications=" + replications +
                    ", bytesProduced=" + bytesProduced +
                    ", bytesFetched=" + bytesFetched +
                    ", avgProduceMs=" + averageProduceLatencyMillis() +
                    ", avgFetchMs=" + averageFetchLatencyMillis() +
                    '}';
        }
    }
}
