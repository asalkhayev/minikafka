package dev.ayan.client;

import dev.ayan.Protocol;
import dev.ayan.log.LogEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Consumer implements AutoCloseable {

    private final String host;
    private final int port;
    private final String topicName;
    private final int partitionId;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private long currentOffset = 0;

    public Consumer(String host, int port, String topicName, int partitionId) throws IOException {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host must not be empty");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id must not be negative");
        }

        validateAndEncodeTopic(topicName);

        this.host = host;
        this.port = port;
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    public synchronized List<LogEntry> poll(int maxMessages) throws IOException {
        if (maxMessages < 0) {
            throw new IllegalArgumentException("Max messages must not be negative");
        }
        if (maxMessages == 0) {
            return Collections.emptyList();
        }
        if (maxMessages > Protocol.MAX_FETCH_ENTRIES) {
            throw new IllegalArgumentException("Max messages is too large: " + maxMessages);
        }

        byte[] topicBytes = validateAndEncodeTopic(topicName);

        out.writeByte(Protocol.CMD_FETCH);
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partitionId);
        out.writeLong(currentOffset);
        out.writeInt(maxMessages);
        out.flush();

        byte responseCode = in.readByte();
        if (responseCode != Protocol.RES_OK) {
            throw new IOException("Broker rejected FETCH request for topic " + topicName);
        }

        int entryCount = in.readInt();
        if (entryCount < 0 || entryCount > Protocol.MAX_FETCH_ENTRIES) {
            throw new IOException("Broker returned invalid entry count: " + entryCount);
        }

        List<LogEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            long offset = in.readLong();
            long timestamp = in.readLong();
            int payloadLength = in.readInt();
            if (payloadLength < 0 || payloadLength > Protocol.MAX_PAYLOAD_BYTES) {
                throw new IOException("Broker returned invalid payload length: " + payloadLength);
            }

            byte[] payload = new byte[payloadLength];
            in.readFully(payload);
            entries.add(new LogEntry(offset, timestamp, payload));
        }

        if (!entries.isEmpty()) {
            currentOffset = entries.get(entries.size() - 1).getOffset() + 1;
        }

        entries.forEach(entry ->
                System.out.println("[Consumer] Read " + topicName + "-" + partitionId +
                        " offset " + entry.getOffset() + ": \"" +
                        new String(entry.getPayload(), StandardCharsets.UTF_8) + "\"")
        );

        return Collections.unmodifiableList(entries);
    }

    public void seek(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        this.currentOffset = offset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private byte[] validateAndEncodeTopic(String topicName) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }

        byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > Protocol.MAX_TOPIC_NAME_BYTES) {
            throw new IllegalArgumentException("Topic name is too long: " + topicBytes.length + " bytes");
        }
        return topicBytes;
    }
}