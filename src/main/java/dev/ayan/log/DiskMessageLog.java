package dev.ayan.log;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DiskMessageLog {

    // Each record on disk looks like:
    // [8 bytes offset][8 bytes timestamp][4 bytes payload length][N bytes payload]
    private static final int HEADER_SIZE = 8 + 8 + 4; // 20 bytes

    private final File logFile;
    private long nextOffset = 0;

    public DiskMessageLog(String directory, String topicName, int partitionId) throws IOException {
        File dir = new File(directory);
        if (!dir.exists()) dir.mkdirs();

        this.logFile = new File(dir, topicName + "-" + partitionId + ".log");

        // If file already exists, count existing entries so we continue from the right offset
        if (logFile.exists()) {
            nextOffset = countEntries();
        }
    }

    // Append a message to disk, return the offset assigned
    public synchronized long append(byte[] payload) throws IOException {
        long offset = nextOffset;
        long timestamp = System.currentTimeMillis();

        // Allocate a buffer: header + payload
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payload.length);
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        buffer.putInt(payload.length);
        buffer.put(payload);

        // Append to file (true = append mode, don't overwrite)
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            fos.write(buffer.array());
        }

        nextOffset++;
        return offset;
    }

    // Read up to maxEntries messages starting from fromOffset
    public List<LogEntry> read(long fromOffset, int maxEntries) throws IOException {
        List<LogEntry> result = new ArrayList<>();

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(logFile)))) {

            while (result.size() < maxEntries) {
                // Try to read the fixed header
                byte[] header = new byte[HEADER_SIZE];
                int bytesRead = dis.read(header);
                if (bytesRead < HEADER_SIZE) break; // end of file

                ByteBuffer buf = ByteBuffer.wrap(header);
                long offset    = buf.getLong();
                long timestamp = buf.getLong();
                int length     = buf.getInt();

                // Read the payload
                byte[] payload = new byte[length];
                dis.readFully(payload);

                // Only collect entries at or after fromOffset
                if (offset >= fromOffset) {
                    result.add(new LogEntry(offset, payload));
                }
            }
        }

        return Collections.unmodifiableList(result);
    }

    public long size() { return nextOffset; }

    // Count how many records are already in the file on startup
    private long countEntries() throws IOException {
        long count = 0;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(logFile)))) {
            while (true) {
                byte[] header = new byte[HEADER_SIZE];
                int bytesRead = dis.read(header);
                if (bytesRead < HEADER_SIZE) break;

                ByteBuffer buf = ByteBuffer.wrap(header);
                buf.getLong(); // offset
                buf.getLong(); // timestamp
                int length = buf.getInt();
                dis.skipBytes(length); // skip payload
                count++;
            }
        }
        return count;
    }
}