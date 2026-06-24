package dev.ayan.server;

import dev.ayan.Protocol;
import dev.ayan.broker.ConsumerGroupManager;
import dev.ayan.broker.Topic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerServer {

    private final int port;
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final ConsumerGroupManager consumerGroupManager = new ConsumerGroupManager();
    private final String dataDirectory;
    private volatile boolean running;
    private volatile ServerSocket serverSocket;

    public BrokerServer(int port, String dataDirectory) {
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (dataDirectory == null || dataDirectory.isBlank()) {
            throw new IllegalArgumentException("Data directory must not be empty");
        }

        this.port = port;
        this.dataDirectory = dataDirectory;
    }

    // Create a topic on the broker
    public void createTopic(String name, int partitions) throws IOException {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }

        Topic previous = topics.putIfAbsent(name, new Topic(name, partitions, dataDirectory));
        if (previous != null) {
            System.out.println("[Broker] Topic already exists: " + name);
            return;
        }

        System.out.println("[Broker] Topic created: " + name +
                " with " + partitions + " partition(s)");
    }

    // Start listening for connections
    public void start() throws IOException {
        running = true;
        try (ServerSocket openedServerSocket = new ServerSocket(port)) {
            serverSocket = openedServerSocket;
            System.out.println("[Broker] Listening on port " + port);

            // Accept connections until the broker is stopped
            while (running) {
                try {
                    Socket clientSocket = openedServerSocket.accept();
                    System.out.println("[Broker] Client connected: " +
                            clientSocket.getInetAddress());

                    // Handle each client in its own thread so multiple clients can connect
                    Thread clientThread = new Thread(() -> handleClient(clientSocket),
                            "broker-client-" + clientSocket.getPort());
                    clientThread.start();
                } catch (SocketException e) {
                    if (running) {
                        throw e;
                    }
                    break;
                }
            }
        } finally {
            running = false;
            serverSocket = null;
            System.out.println("[Broker] Stopped.");
        }
    }

    public void stop() throws IOException {
        running = false;
        ServerSocket socketToClose = serverSocket;
        if (socketToClose != null && !socketToClose.isClosed()) {
            socketToClose.close();
        }
    }

    private void handleClient(Socket socket) {
        try (
                socket;
                DataInputStream in = new DataInputStream(socket.getInputStream());
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
                } else if (command == Protocol.CMD_COMMIT_OFFSET) {
                    handleCommitOffset(in, out);
                } else if (command == Protocol.CMD_GET_OFFSET) {
                    handleGetOffset(in, out);
                } else {
                    System.out.println("[Broker] Unknown command: " + command);
                    out.writeByte(Protocol.RES_ERROR);
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("[Broker] Client error: " + e.getMessage());
        }
    }

    // SEND request format:
    // [1 byte cmd][2 bytes topic length][N bytes topic][4 bytes partition][4 bytes payload length][N bytes payload]
    private void handleSend(DataInputStream in, DataOutputStream out) throws IOException {
        String topicName = readTopicName(in);

        int partitionId = in.readInt();
        if (partitionId < 0) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        int payloadLength = in.readInt();
        if (payloadLength < 0 || payloadLength > Protocol.MAX_PAYLOAD_BYTES) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        byte[] payload = new byte[payloadLength];
        in.readFully(payload);

        Topic topic = topics.get(topicName);
        if (topic == null) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        try {
            long offset = topic.getPartition(partitionId).append(payload);
            out.writeByte(Protocol.RES_OK);
            out.writeLong(offset); // send back the assigned offset
            out.flush();
        } catch (IllegalArgumentException e) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
        }
    }

    // FETCH request format:
    // [1 byte cmd][2 bytes topic length][N bytes topic][4 bytes partition][8 bytes fromOffset][4 bytes maxEntries]
    private void handleFetch(DataInputStream in, DataOutputStream out) throws IOException {
        String topicName = readTopicName(in);

        int partitionId = in.readInt();
        long fromOffset = in.readLong();
        int maxEntries = in.readInt();

        if (partitionId < 0 || fromOffset < 0 || maxEntries < 0 || maxEntries > Protocol.MAX_FETCH_ENTRIES) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        Topic topic = topics.get(topicName);
        if (topic == null) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        try {
            var entries = topic.getPartition(partitionId).read(fromOffset, maxEntries);

            out.writeByte(Protocol.RES_OK);
            out.writeInt(entries.size()); // how many entries follow

            for (var entry : entries) {
                byte[] payload = entry.getPayload();
                out.writeLong(entry.getOffset());
                out.writeLong(entry.getTimestamp());
                out.writeInt(payload.length);
                out.write(payload);
            }
            out.flush();
        } catch (IllegalArgumentException e) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
        }
    }

    private String readTopicName(DataInputStream in) throws IOException {
        int topicNameLength = Short.toUnsignedInt(in.readShort());
        if (topicNameLength == 0 || topicNameLength > Protocol.MAX_TOPIC_NAME_BYTES) {
            throw new IOException("Invalid topic name length: " + topicNameLength);
        }

        byte[] topicNameBytes = new byte[topicNameLength];
        in.readFully(topicNameBytes);
        return new String(topicNameBytes, StandardCharsets.UTF_8);
    }

    // COMMIT_OFFSET request format:
    // [1 byte cmd][2 bytes group length][N bytes group][2 bytes topic length][N bytes topic][4 bytes partition][8 bytes offset]
    private void handleCommitOffset(DataInputStream in, DataOutputStream out) throws IOException {
        String groupId = readGroupId(in);
        String topicName = readTopicName(in);
        int partitionId = in.readInt();
        long offset = in.readLong();

        if (partitionId < 0 || offset < 0) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        Topic topic = topics.get(topicName);
        if (topic == null || partitionId >= topic.getPartitionCount()) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        try {
            consumerGroupManager.commitOffset(groupId, topicName, partitionId, offset);
            out.writeByte(Protocol.RES_OK);
            out.flush();
        } catch (IllegalArgumentException e) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
        }
    }

    // GET_OFFSET request format:
    // [1 byte cmd][2 bytes group length][N bytes group][2 bytes topic length][N bytes topic][4 bytes partition]
    private void handleGetOffset(DataInputStream in, DataOutputStream out) throws IOException {
        String groupId = readGroupId(in);
        String topicName = readTopicName(in);
        int partitionId = in.readInt();

        if (partitionId < 0) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        Topic topic = topics.get(topicName);
        if (topic == null || partitionId >= topic.getPartitionCount()) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
            return;
        }

        try {
            long committedOffset = consumerGroupManager.getCommittedOffset(groupId, topicName, partitionId);
            out.writeByte(Protocol.RES_OK);
            out.writeLong(committedOffset);
            out.flush();
        } catch (IllegalArgumentException e) {
            out.writeByte(Protocol.RES_ERROR);
            out.flush();
        }
    }

    private String readGroupId(DataInputStream in) throws IOException {
        int groupIdLength = Short.toUnsignedInt(in.readShort());
        if (groupIdLength == 0 || groupIdLength > Protocol.MAX_GROUP_ID_BYTES) {
            throw new IOException("Invalid group id length: " + groupIdLength);
        }

        byte[] groupIdBytes = new byte[groupIdLength];
        in.readFully(groupIdBytes);
        return new String(groupIdBytes, StandardCharsets.UTF_8);
    }
}