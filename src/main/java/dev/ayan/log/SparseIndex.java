package dev.ayan.log;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;

public class SparseIndex {

    private static final int INDEX_ENTRY_SIZE = Long.BYTES + Long.BYTES;

    private final File indexFile;
    private final NavigableMap<Long, Long> offsetToPosition = new TreeMap<>();

    public SparseIndex(String directory, String topicName, int partitionId) throws IOException {
        if (directory == null || directory.isBlank()) {
            throw new IllegalArgumentException("Index directory must not be empty");
        }
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id must not be negative");
        }

        File dir = new File(directory);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Could not create index directory: " + dir.getAbsolutePath());
        }
        if (!dir.isDirectory()) {
            throw new IOException("Index path is not a directory: " + dir.getAbsolutePath());
        }

        this.indexFile = new File(dir, topicName + "-" + partitionId + ".index");
        loadExistingIndex();
    }

    public synchronized void add(long offset, long bytePosition) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        if (bytePosition < 0) {
            throw new IllegalArgumentException("Byte position must not be negative");
        }

        offsetToPosition.put(offset, bytePosition);

        ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        buffer.putLong(offset);
        buffer.putLong(bytePosition);

        try (FileOutputStream fos = new FileOutputStream(indexFile, true)) {
            fos.write(buffer.array());
            fos.getFD().sync();
        }
    }

    public synchronized OptionalLong floorPosition(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }

        var entry = offsetToPosition.floorEntry(offset);
        if (entry == null) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(entry.getValue());
    }

    public synchronized int size() {
        return offsetToPosition.size();
    }

    private void loadExistingIndex() throws IOException {
        if (!indexFile.exists()) {
            return;
        }

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(indexFile)))) {
            while (true) {
                try {
                    long offset = dis.readLong();
                    long bytePosition = dis.readLong();

                    if (offset < 0 || bytePosition < 0) {
                        throw new IOException("Corrupted index entry in " + indexFile.getAbsolutePath());
                    }

                    offsetToPosition.put(offset, bytePosition);
                } catch (EOFException e) {
                    break;
                }
            }
        }
    }
}
