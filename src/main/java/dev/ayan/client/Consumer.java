package dev.ayan.client;

import dev.ayan.broker.Topic;
import dev.ayan.log.LogEntry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class Consumer {
    private final Topic topic;
    private final int partitionId;
    private long currentOffset = 0;

    public Consumer(Topic topic, int partitionId) {
        this.topic = topic;
        this.partitionId = partitionId;
    }

    public List<LogEntry> poll(int maxMessages) throws IOException {
        List<LogEntry> entries = topic
                .getPartition(partitionId)
                .read(currentOffset, maxMessages);

        if (!entries.isEmpty()) {
            currentOffset = entries.get(entries.size() - 1).getOffset() + 1;
        }

        entries.forEach(e ->
                System.out.println("[Consumer] Read offset " + e.getOffset() +
                        ": \"" + new String(e.getPayload()) + "\"")
        );
        return entries;
    }

    public long getCurrentOffset() { return currentOffset; }
}