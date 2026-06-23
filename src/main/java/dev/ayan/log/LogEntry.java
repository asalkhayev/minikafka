package dev.ayan.log;

public class LogEntry {
    private final long offset;
    private final long timestamp;
    private final byte[] payload;

    public LogEntry(long offset, byte[] payload) {
        this.offset = offset;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    public long getOffset()     { return offset; }
    public long getTimestamp()  { return timestamp; }
    public byte[] getPayload()  { return payload; }

    @Override
    public String toString() {
        return "LogEntry{offset=" + offset +
                ", payload=\"" + new String(payload) + "\"}";
    }
}