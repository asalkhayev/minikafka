package dev.ayan.broker;

import dev.ayan.log.DiskMessageLog;
import dev.ayan.log.LogEntry;
import java.io.IOException;
import java.util.List;

public class Partition {
    private final int id;
    private final DiskMessageLog log;

    public Partition(int id, String dataDirectory, String topicName) throws IOException {
        this.id = id;
        this.log = new DiskMessageLog(dataDirectory, topicName, id);
    }

    public long append(byte[] payload) throws IOException {
        return log.append(payload);
    }

    public List<LogEntry> read(long fromOffset, int maxEntries) throws IOException {
        return log.read(fromOffset, maxEntries);
    }

    public int getId() { return id; }
    public long size() { return log.size(); }
}