package com.kafka.broker;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ClientConnection implements Runnable {

    private final int connectionId;
    private final Socket socket;


    public ClientConnection(int connectionId, Socket socket) {
        this.connectionId = connectionId;
        this.socket = socket;
    }

    @Override
    public void run() {

        System.out.println("Connection #" + connectionId + " handled by " + Thread.currentThread().getName());

        try (
                Socket client = socket;
                InputStream input = client.getInputStream()
        ) {
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = input.read(buffer)) != -1) {
                System.out.println("Connection #" + connectionId + " received " + bytesRead + " bytes");
            }
        } catch (IOException e) {
            System.out.println("Connection #" + connectionId + " closed: " + e.getMessage());

        } finally {
            System.out.println("Connection #" + connectionId + " worker finished.");
        }
    }
}
