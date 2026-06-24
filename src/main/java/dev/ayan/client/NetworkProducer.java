package dev.ayan.client;

import dev.ayan.Protocol;

import java.io.*;
import java.net.Socket;

public class NetworkProducer {
    private final String host;
    private final int port;

    public NetworkProducer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public long send(String topic, int partitionId, String message) throws IOException {
        try (
                Socket socket = new Socket(host, port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream  in  = new DataInputStream(socket.getInputStream())
        ) {
            byte[] topicBytes   = topic.getBytes();
            byte[] payload      = message.getBytes();

            out.writeByte(Protocol.CMD_SEND);
            out.writeShort(topicBytes.length);
            out.write(topicBytes);
            out.writeInt(partitionId);
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();

            byte status = in.readByte();
            if (status != Protocol.RES_OK) throw new IOException("Broker returned error");
            long offset = in.readLong();
            System.out.println("[NetworkProducer] Sent \"" + message + "\" at offset " + offset);
            return offset;
        }
    }
}