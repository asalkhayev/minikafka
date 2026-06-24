package dev.ayan.log;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LogEntry {
    private final long offset;
    private final long timestamp;
    private final byte[] payload;

    public LogEntry(long offset, byte[] payload) {
        this(offset, System.currentTimeMillis(), payload);
    }

    public LogEntry(long offset, long timestamp, byte[] payload) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp must not be negative");
        }
        if (payload == null) {
            throw new IllegalArgumentException("Payload must not be null");
        }

        this.offset = offset;
        this.timestamp = timestamp;
        this.payload = Arrays.copyOf(payload, payload.length);
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    @Override
    public String toString() {
        return "LogEntry{offset=" + offset +
                ", timestamp=" + timestamp +
                ", payload=\"" + new String(payload, StandardCharsets.UTF_8) + "\"}";
    }
}