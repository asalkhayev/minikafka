package dev.ayan.client;

import dev.ayan.broker.Topic;
import java.io.IOException;

public class Producer {
    private final Topic topic;

    public Producer(Topic topic) { this.topic = topic; }

    public long send(String message, int partitionId) throws IOException {
        byte[] payload = message.getBytes();
        long offset = topic.getPartition(partitionId).append(payload);
        System.out.println("[Producer] Sent \"" + message +
                "\" to " + topic.getName() +
                "-" + partitionId +
                " at offset " + offset);
        return offset;
    }
}