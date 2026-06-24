package dev.ayan.client;

import dev.ayan.Protocol;
import dev.ayan.log.LogEntry;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class NetworkConsumer {
    private final String host;
    private final int port;
    private final String topic;
    private final int partitionId;
    private long currentOffset = 0;

    public NetworkConsumer(String host, int port, String topic, int partitionId) {
        this.host        = host;
        this.port        = port;
        this.topic       = topic;
        this.partitionId = partitionId;
    }

    public List<LogEntry> poll(int maxMessages) throws IOException {
        try (
                Socket socket = new Socket(host, port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream  in  = new DataInputStream(socket.getInputStream())
        ) {
            byte[] topicBytes = topic.getBytes();

            out.writeByte(Protocol.CMD_FETCH);
            out.writeShort(topicBytes.length);
            out.write(topicBytes);
            out.writeInt(partitionId);
            out.writeLong(currentOffset);
            out.writeInt(maxMessages);
            out.flush();

            byte status = in.readByte();
            if (status != Protocol.RES_OK) throw new IOException("Broker returned error");

            int count = in.readInt();
            List<LogEntry> entries = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                long offset    = in.readLong();
                long timestamp = in.readLong();
                int length     = in.readInt();
                byte[] payload = new byte[length];
                in.readFully(payload);
                entries.add(new LogEntry(offset, payload));
            }

            if (!entries.isEmpty()) {
                currentOffset = entries.get(entries.size() - 1).getOffset() + 1;
            }

            entries.forEach(e ->
                    System.out.println("[NetworkConsumer] offset " + e.getOffset() +
                            ": \"" + new String(e.getPayload()) + "\"")
            );
            return entries;
        }
    }

    public long getCurrentOffset() { return currentOffset; }
}