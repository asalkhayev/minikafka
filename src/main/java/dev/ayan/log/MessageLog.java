package dev.ayan.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MessageLog {
    private final List<LogEntry> entries = new ArrayList<>();

    public synchronized long append(byte[] payload) {
        long offset = entries.size();
        entries.add(new LogEntry(offset, payload));
        return offset;
    }

    public List<LogEntry> read(long fromOffset, int maxEntries) {
        if (fromOffset >= entries.size()) {
            return Collections.emptyList();
        }
        int to = (int) Math.min(fromOffset + maxEntries, entries.size());
        return Collections.unmodifiableList(
                entries.subList((int) fromOffset, to)
        );
    }

    public long size() { return entries.size(); }
}