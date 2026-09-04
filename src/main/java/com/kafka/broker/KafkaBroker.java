package com.kafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaBroker {

    private final int port;
    private final int workerThreads;

    private final AtomicInteger connectionCounter = new AtomicInteger(0);

    private final ExecutorService connectionPool;

    private volatile boolean running;

    private volatile ServerSocket serverSocket;

    public KafkaBroker(int port, int workerThreads) {

        if (workerThreads <= 0) {
            throw new IllegalArgumentException("Worker thread count must be positive");
        }

        this.port = port;
        this.workerThreads = workerThreads;

        this.connectionPool = Executors.newFixedThreadPool(workerThreads);
    }

    public void start() throws IOException {

        serverSocket = new ServerSocket(port);
        running = true;

        System.out.println("SimpleKafka broker started on port " + serverSocket.getLocalPort());

        System.out.println("Connection worker threads: " + workerThreads);

        try {
            while (running) {
                Socket clientSocket = serverSocket.accept();

                int connectionId = connectionCounter.incrementAndGet();

                System.out.println("Accepted connection #"
                                + connectionId
                                + " from "
                                + clientSocket.getRemoteSocketAddress()
                );

                configureSocket(clientSocket);

                connectionPool.submit(
                        new ClientConnection(connectionId, clientSocket));
            }

        } catch (IOException e) {
            if (running) throw e;
        } finally {
            shutdown();
        }
    }

    private void configureSocket(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    public int getPort() {
        ServerSocket currentSocket = serverSocket;

        if (currentSocket == null) return -1;

        return currentSocket.getLocalPort();
    }

    public void shutdown() {

        if (!running) return;
        running = false;

        System.out.println("Shutting down broker...");

        ServerSocket currentSocket = serverSocket;

        if (currentSocket != null) {
            try {
                currentSocket.close();
            } catch (IOException ignored) {
            }
        }

        connectionPool.shutdown();

        System.out.println("Broker stopped.");
    }

    public static void main(String[] args) throws IOException {

        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9092;

        int workers = args.length > 1 ? Integer.parseInt(args[1]) : 10;

        KafkaBroker broker = new KafkaBroker(port, workers);

        Runtime.getRuntime().addShutdownHook(new Thread(broker::shutdown));

        broker.start();
    }
}