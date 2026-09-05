package com.kafka.benchmark;

import com.kafka.broker.KafkaBroker;
import com.kafka.protocol.ProduceRequest;
import com.kafka.protocol.ProtocolDecoder;
import com.kafka.protocol.ProtocolEncoder;
import com.kafka.protocol.ProtocolFrame;
import com.kafka.protocol.RequestType;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class BrokerBenchmark {

    private BrokerBenchmark() {}

    public static void main(String[] args) throws Exception {
        int messages = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        int messageSize = args.length > 1 ? Integer.parseInt(args[1]) : 128;
        int clients = args.length > 2 ? Integer.parseInt(args[2]) : 4;

        if (messages <= 0 || messageSize < 0 || clients <= 0) {
            throw new IllegalArgumentException(
                    "Usage: BrokerBenchmark [messages] [messageBytes] [clients]"
            );
        }

        Path data = Files.createTempDirectory("kafka-benchmark-");
        KafkaBroker broker = new KafkaBroker(0, Math.max(clients, 4), data);

        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Exception ignored) {
            }
        }, "benchmark-broker");

        brokerThread.start();
        waitForBroker(broker);

        byte[] payload = new byte[messageSize];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = 'x';
        }

        int workerCount = Math.min(clients, messages);
        ExecutorService workers = Executors.newFixedThreadPool(workerCount);
        List<Future<List<Long>>> futures = new ArrayList<>();

        int baseMessages = messages / workerCount;
        int remainder = messages % workerCount;

        long start = System.nanoTime();

        try {
            for (int client = 0; client < workerCount; client++) {
                int count = baseMessages + (client < remainder ? 1 : 0);
                futures.add(workers.submit(
                        producer(broker.getPort(), client, count, payload)
                ));
            }

            List<Long> latencies = new ArrayList<>(messages);
            for (Future<List<Long>> future : futures) {
                latencies.addAll(future.get());
            }

            double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
            Collections.sort(latencies);

            System.out.printf(
                    "messages=%d messageBytes=%d clients=%d seconds=%.3f throughput=%.2f msg/s%n",
                    messages, messageSize, workerCount, seconds, messages / seconds
            );
            System.out.printf(
                    "latencyMs p50=%.3f p95=%.3f p99=%.3f%n",
                    percentileMillis(latencies, 0.50),
                    percentileMillis(latencies, 0.95),
                    percentileMillis(latencies, 0.99)
            );
            System.out.println(broker.getMetrics());

        } finally {
            workers.shutdownNow();
            broker.shutdown();
            brokerThread.join(2000);
        }
    }

    private static Callable<List<Long>> producer(
            int port,
            int clientId,
            int messages,
            byte[] payload) {

        return () -> {
            List<Long> latencies = new ArrayList<>(messages);

            try (Socket socket = new Socket("127.0.0.1", port);
                 InputStream input = socket.getInputStream();
                 OutputStream output = socket.getOutputStream()) {

                ProtocolEncoder encoder = new ProtocolEncoder(output);
                ProtocolDecoder decoder = new ProtocolDecoder(input);

                for (int i = 0; i < messages; i++) {
                    ProduceRequest body =
                            new ProduceRequest("benchmark-" + clientId, 0, payload);

                    ProtocolFrame request =
                            new ProtocolFrame(
                                    ProtocolFrame.CURRENT_VERSION,
                                    RequestType.PRODUCE,
                                    ((long) clientId << 32) | i,
                                    body.encode()
                            );

                    long started = System.nanoTime();
                    encoder.writeFrame(request);

                    ProtocolFrame response = decoder.readFrame();
                    long offset = ByteBuffer.wrap(response.getPayload()).getLong();

                    if (offset < 0) {
                        throw new IllegalStateException(
                                "Broker returned invalid offset: " + offset
                        );
                    }

                    latencies.add(System.nanoTime() - started);
                }
            }

            return latencies;
        };
    }

    private static double percentileMillis(List<Long> sortedNanos, double percentile) {
        if (sortedNanos.isEmpty()) {
            return 0.0;
        }

        int index = (int) Math.ceil(percentile * sortedNanos.size()) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.size() - 1));

        return sortedNanos.get(index) / 1_000_000.0;
    }

    private static void waitForBroker(KafkaBroker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;

        while (broker.getPort() <= 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        if (broker.getPort() <= 0) {
            throw new IllegalStateException("Broker did not start");
        }
    }
}
