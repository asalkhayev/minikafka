package dev.ayan.broker;

import dev.ayan.log.DiskMessageLog;
import dev.ayan.log.LogEntry;
import java.io.IOException;
import java.util.List;

public class Partition {
    private final int id;
    private final DiskMessageLog log;

    public Partition(int id, String dataDirectory, String topicName) throws IOException {
        if (id < 0) {
            throw new IllegalArgumentException("Partition id must not be negative");
        }
        if (dataDirectory == null || dataDirectory.isBlank()) {
            throw new IllegalArgumentException("Data directory must not be empty");
        }
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }

        this.id = id;
        this.log = new DiskMessageLog(dataDirectory, topicName, id);
    }

    public synchronized long append(byte[] payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("Payload must not be null");
        }
        return log.append(payload);
    }

    public synchronized List<LogEntry> read(long fromOffset, int maxEntries) throws IOException {
        if (fromOffset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        if (maxEntries < 0) {
            throw new IllegalArgumentException("Max entries must not be negative");
        }
        return log.read(fromOffset, maxEntries);
    }

    public int getId() {
        return id;
    }

    public synchronized long size() {
        return log.size();
    }
}