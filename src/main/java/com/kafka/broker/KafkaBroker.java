package com.kafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaBroker {

    private final int brokerId;
    private final int port;
    private final int workerThreads;

    private final AtomicInteger connectionCounter =
            new AtomicInteger(0);

    private final ExecutorService connectionPool;

    private final TopicManager topicManager;

    private final ClusterMetadata clusterMetadata;

    private final ReplicationManager replicationManager;

    private volatile boolean running;

    private volatile ServerSocket serverSocket;

    public KafkaBroker(int port, int workerThreads)
            throws IOException {

        this(
                0,
                port,
                workerThreads,
                Paths.get("data")
        );
    }

    public KafkaBroker(
            int port,
            int workerThreads,
            Path dataDirectory)
            throws IOException {

        this(
                0,
                port,
                workerThreads,
                dataDirectory
        );
    }

    public KafkaBroker(
            int brokerId,
            int port,
            int workerThreads,
            Path dataDirectory)
            throws IOException {

        if (brokerId < 0) {
            throw new IllegalArgumentException(
                    "Broker ID cannot be negative"
            );
        }

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "Invalid broker port: " + port
            );
        }

        if (workerThreads <= 0) {
            throw new IllegalArgumentException(
                    "Worker thread count must be positive"
            );
        }

        this.brokerId = brokerId;
        this.port = port;
        this.workerThreads = workerThreads;

        this.connectionPool =
                Executors.newFixedThreadPool(workerThreads);

        this.topicManager =
                new TopicManager(dataDirectory);

        /*
         * Both KafkaBroker and ReplicationManager must
         * reference the SAME ClusterMetadata instance.
         */
        this.clusterMetadata =
                new ClusterMetadata();

        this.replicationManager =
                new ReplicationManager(
                        brokerId,
                        dataDirectory,
                        clusterMetadata
                );
    }

    public void start() throws IOException {

        serverSocket = new ServerSocket(port);

        running = true;

        System.out.println(
                "SimpleKafka broker "
                        + brokerId
                        + " started on port "
                        + serverSocket.getLocalPort()
        );

        System.out.println(
                "Connection worker threads: "
                        + workerThreads
        );

        try {

            while (running) {

                Socket clientSocket =
                        serverSocket.accept();

                int connectionId =
                        connectionCounter
                                .incrementAndGet();

                System.out.println(
                        "Accepted connection #"
                                + connectionId
                                + " from "
                                + clientSocket
                                .getRemoteSocketAddress()
                );

                configureSocket(clientSocket);

                connectionPool.submit(
                        new ClientConnection(
                                connectionId,
                                clientSocket,
                                topicManager,
                                replicationManager,
                                clusterMetadata
                        )
                );
            }

        } catch (IOException e) {

            if (running) {
                throw e;
            }

        } finally {

            shutdown();
        }
    }

    private void configureSocket(
            Socket socket)
            throws IOException {

        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPort() {

        ServerSocket currentSocket =
                serverSocket;

        if (currentSocket == null) {
            return -1;
        }

        return currentSocket.getLocalPort();
    }

    public TopicManager getTopicManager() {
        return topicManager;
    }

    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    public ClusterMetadata getClusterMetadata() {
        return clusterMetadata;
    }

    public void shutdown() {

        if (!running) {
            return;
        }

        running = false;

        System.out.println(
                "Shutting down broker..."
        );

        ServerSocket currentSocket =
                serverSocket;

        if (currentSocket != null) {

            try {
                currentSocket.close();

            } catch (IOException ignored) {
            }
        }

        connectionPool.shutdown();

        try {

            replicationManager.close();

        } catch (IOException e) {

            System.out.println(
                    "Error closing replication manager: "
                            + e.getMessage()
            );
        }

        try {

            topicManager.close();

        } catch (IOException e) {

            System.out.println(
                    "Error closing topic manager: "
                            + e.getMessage()
            );
        }

        System.out.println(
                "Broker stopped."
        );
    }

    public static void main(
            String[] args)
            throws IOException {

        int brokerId =
                args.length > 0
                        ? Integer.parseInt(args[0])
                        : 0;

        int port =
                args.length > 1
                        ? Integer.parseInt(args[1])
                        : 9092;

        int workers =
                args.length > 2
                        ? Integer.parseInt(args[2])
                        : 10;

        Path dataDirectory =
                args.length > 3
                        ? Paths.get(args[3])
                        : Paths.get(
                        "data-broker-" + brokerId
                );

        KafkaBroker broker =
                new KafkaBroker(
                        brokerId,
                        port,
                        workers,
                        dataDirectory
                );

        Runtime.getRuntime().addShutdownHook(
                new Thread(broker::shutdown)
        );

        broker.start();
    }
}