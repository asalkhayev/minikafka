package dev.ayan.log;

import dev.ayan.Protocol;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

public class DiskMessageLog {

    // Each record on disk looks like:
    // [8 bytes offset][8 bytes timestamp][4 bytes payload length][N bytes payload]
    private static final int HEADER_SIZE = 8 + 8 + 4; // 20 bytes
    private static final int INDEX_INTERVAL = 100;

    private final File logFile;
    private final SparseIndex sparseIndex;
    private long nextOffset = 0;

    public DiskMessageLog(String directory, String topicName, int partitionId) throws IOException {
        if (directory == null || directory.isBlank()) {
            throw new IllegalArgumentException("Log directory must not be empty");
        }
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id must not be negative");
        }

        File dir = new File(directory);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Could not create log directory: " + dir.getAbsolutePath());
        }
        if (!dir.isDirectory()) {
            throw new IOException("Log path is not a directory: " + dir.getAbsolutePath());
        }

        this.logFile = new File(dir, topicName + "-" + partitionId + ".log");
        this.sparseIndex = new SparseIndex(directory, topicName, partitionId);

        // If file already exists, count existing entries so we continue from the right offset
        if (logFile.exists()) {
            nextOffset = countEntries();
        }
    }

    // Append a message to disk, return the offset assigned
    public synchronized long append(byte[] payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("Payload must not be null");
        }
        if (payload.length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Payload is too large: " + payload.length + " bytes");
        }

        long offset = nextOffset;
        long timestamp = System.currentTimeMillis();
        long bytePosition = logFile.exists() ? logFile.length() : 0;

        // Allocate a buffer: header + payload
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payload.length);
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        buffer.putInt(payload.length);
        buffer.put(payload);

        // Append to file (true = append mode, don't overwrite)
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            fos.write(buffer.array());
            fos.getFD().sync();
        }

        if (offset % INDEX_INTERVAL == 0) {
            sparseIndex.add(offset, bytePosition);
        }

        nextOffset++;
        return offset;
    }

    // Read up to maxEntries messages starting from fromOffset
    public synchronized List<LogEntry> read(long fromOffset, int maxEntries) throws IOException {
        if (fromOffset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        if (maxEntries < 0) {
            throw new IllegalArgumentException("Max entries must not be negative");
        }
        if (maxEntries > Protocol.MAX_FETCH_ENTRIES) {
            throw new IllegalArgumentException("Max entries is too large: " + maxEntries);
        }
        if (!logFile.exists() || maxEntries == 0 || fromOffset >= nextOffset) {
            return Collections.emptyList();
        }

        List<LogEntry> result = new ArrayList<>();
        long startPosition = findStartPosition(fromOffset);

        try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
            raf.seek(startPosition);

            while (result.size() < maxEntries) {
                LogEntry entry = readEntry(raf);
                if (entry == null) {
                    break;
                }

                // Only collect entries at or after fromOffset
                if (entry.getOffset() >= fromOffset) {
                    result.add(entry);
                }
            }
        }

        return Collections.unmodifiableList(result);
    }

    public synchronized long size() {
        return nextOffset;
    }

    // Count how many complete records are already in the file on startup
    private long countEntries() throws IOException {
        long count = 0;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(logFile)))) {
            while (readEntry(dis) != null) {
                count++;
            }
        }
        return count;
    }

    private long findStartPosition(long fromOffset) {
        OptionalLong indexedPosition = sparseIndex.floorPosition(fromOffset);
        return indexedPosition.orElse(0L);
    }

    private LogEntry readEntry(DataInputStream dis) throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        try {
            dis.readFully(header);
        } catch (EOFException e) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(header);
        long offset = buf.getLong();
        long timestamp = buf.getLong();
        int length = buf.getInt();

        if (length < 0 || length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new IOException("Corrupted log record payload length: " + length);
        }

        byte[] payload = new byte[length];
        try {
            dis.readFully(payload);
        } catch (EOFException e) {
            return null;
        }

        return new LogEntry(offset, timestamp, payload);
    }

    private LogEntry readEntry(RandomAccessFile raf) throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        try {
            raf.readFully(header);
        } catch (EOFException e) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(header);
        long offset = buf.getLong();
        long timestamp = buf.getLong();
        int length = buf.getInt();

        if (length < 0 || length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new IOException("Corrupted log record payload length: " + length);
        }

        byte[] payload = new byte[length];
        try {
            raf.readFully(payload);
        } catch (EOFException e) {
            return null;
        }

        return new LogEntry(offset, timestamp, payload);
    }
}