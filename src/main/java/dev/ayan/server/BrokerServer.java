package dev.ayan.server;

import dev.ayan.Protocol;
import dev.ayan.broker.Topic;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class BrokerServer {

    private final int port;
    private final Map<String, Topic> topics = new HashMap<>();
    private final String dataDirectory;

    public BrokerServer(int port, String dataDirectory) {
        this.port = port;
        this.dataDirectory = dataDirectory;
    }

    // Create a topic on the broker
    public void createTopic(String name, int partitions) throws IOException {
        topics.put(name, new Topic(name, partitions, dataDirectory));
        System.out.println("[Broker] Topic created: " + name +
                " with " + partitions + " partition(s)");
    }

    // Start listening for connections
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[Broker] Listening on port " + port);

        // Accept connections forever
        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("[Broker] Client connected: " +
                    clientSocket.getInetAddress());

            // Handle each client in its own thread so multiple clients can connect
            Thread clientThread = new Thread(() -> handleClient(clientSocket));
            clientThread.start();
        }
    }

    private void handleClient(Socket socket) {
        try (
                DataInputStream  in  = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream())
        ) {
            while (true) {
                // Read command byte
                byte command;
                try {
                    command = in.readByte();
                } catch (EOFException e) {
                    System.out.println("[Broker] Client disconnected.");
                    break;
                }

                if (command == Protocol.CMD_SEND) {
                    handleSend(in, out);
                } else if (command == Protocol.CMD_FETCH) {
                    handleFetch(in, out);
                } else {
                    System.out.println("[Broker] Unknown command: " + command);
                }
            }
        } catch (IOException e) {
            System.out.println("[Broker] Client error: " + e.getMessage());
        }
    }

    // SEND request format:
    // [1 byte cmd][2 bytes topic length][N bytes topic][4 bytes partition][4 bytes payload length][N bytes payload]
    private void handleSend(DataInputStream in, DataOutputStream out) throws IOException {
        short topicNameLength = in.readShort();
        byte[] topicNameBytes = new byte[topicNameLength];
        in.readFully(topicNameBytes);
        String topicName = new String(topicNameBytes);

        int partitionId = in.readInt();

        int payloadLength = in.readInt();
        byte[] payload = new byte[payloadLength];
        in.readFully(payload);

        Topic topic = topics.get(topicName);
        if (topic == null) {
            out.writeByte(Protocol.RES_ERROR);
            return;
        }

        long offset = topic.getPartition(partitionId).append(payload);
        out.writeByte(Protocol.RES_OK);
        out.writeLong(offset); // send back the assigned offset
    }

    // FETCH request format:
    // [1 byte cmd][2 bytes topic length][N bytes topic][4 bytes partition][8 bytes fromOffset][4 bytes maxEntries]
    private void handleFetch(DataInputStream in, DataOutputStream out) throws IOException {
        short topicNameLength = in.readShort();
        byte[] topicNameBytes = new byte[topicNameLength];
        in.readFully(topicNameBytes);
        String topicName = new String(topicNameBytes);

        int partitionId  = in.readInt();
        long fromOffset  = in.readLong();
        int maxEntries   = in.readInt();

        Topic topic = topics.get(topicName);
        if (topic == null) {
            out.writeByte(Protocol.RES_ERROR);
            return;
        }

        var entries = topic.getPartition(partitionId).read(fromOffset, maxEntries);

        out.writeByte(Protocol.RES_OK);
        out.writeInt(entries.size()); // how many entries follow

        for (var entry : entries) {
            out.writeLong(entry.getOffset());
            out.writeLong(entry.getTimestamp());
            out.writeInt(entry.getPayload().length);
            out.write(entry.getPayload());
        }
    }
}