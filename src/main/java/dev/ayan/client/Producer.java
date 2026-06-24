package dev.ayan.client;

import dev.ayan.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Producer implements AutoCloseable {

    private final String host;
    private final int port;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final boolean loggingEnabled;

    public Producer(String host, int port) throws IOException {
        this(host, port, true);
    }

    public Producer(String host, int port, boolean loggingEnabled) throws IOException {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host must not be empty");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }

        this.host = host;
        this.port = port;
        this.loggingEnabled = loggingEnabled;
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    public long send(String topicName, String message, int partitionId) throws IOException {
        if (message == null) {
            throw new IllegalArgumentException("Message must not be null");
        }
        return send(topicName, message.getBytes(StandardCharsets.UTF_8), partitionId);
    }

    public synchronized long send(String topicName, byte[] payload, int partitionId) throws IOException {
        byte[] topicBytes = validateAndEncodeTopic(topicName);
        validatePartition(partitionId);
        validatePayload(payload);

        out.writeByte(Protocol.CMD_SEND);
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partitionId);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();

        byte responseCode = in.readByte();
        if (responseCode != Protocol.RES_OK) {
            throw new IOException("Broker rejected SEND request for topic " + topicName);
        }

        long offset = in.readLong();
        if (loggingEnabled) {
            System.out.println("[Producer] Sent " + payload.length + " byte(s) to " +
                    topicName + "-" + partitionId + " at offset " + offset);
        }
        return offset;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
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

    private void validatePartition(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id must not be negative");
        }
    }

    private void validatePayload(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload must not be null");
        }
        if (payload.length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Payload is too large: " + payload.length + " bytes");
        }
    }
}