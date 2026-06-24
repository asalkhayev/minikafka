package dev.ayan.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiskMessageLogTest {

    @TempDir
    Path tempDir;

    @Test
    void appendAssignsSequentialOffsets() throws IOException {
        DiskMessageLog log = new DiskMessageLog(tempDir.toString(), "payments", 0);

        long firstOffset = log.append(bytes("first"));
        long secondOffset = log.append(bytes("second"));
        long thirdOffset = log.append(bytes("third"));

        assertEquals(0, firstOffset);
        assertEquals(1, secondOffset);
        assertEquals(2, thirdOffset);
        assertEquals(3, log.size());
    }

    @Test
    void readReturnsMessagesFromRequestedOffset() throws IOException {
        DiskMessageLog log = new DiskMessageLog(tempDir.toString(), "payments", 0);
        log.append(bytes("first"));
        log.append(bytes("second"));
        log.append(bytes("third"));

        List<LogEntry> entries = log.read(1, 10);

        assertEquals(2, entries.size());
        assertEquals(1, entries.get(0).getOffset());
        assertEquals("second", text(entries.get(0)));
        assertEquals(2, entries.get(1).getOffset());
        assertEquals("third", text(entries.get(1)));
    }

    @Test
    void readRespectsMaxEntries() throws IOException {
        DiskMessageLog log = new DiskMessageLog(tempDir.toString(), "payments", 0);
        log.append(bytes("first"));
        log.append(bytes("second"));
        log.append(bytes("third"));

        List<LogEntry> entries = log.read(0, 2);

        assertEquals(2, entries.size());
        assertEquals("first", text(entries.get(0)));
        assertEquals("second", text(entries.get(1)));
    }

    @Test
    void readReturnsEmptyListWhenOffsetIsPastEnd() throws IOException {
        DiskMessageLog log = new DiskMessageLog(tempDir.toString(), "payments", 0);
        log.append(bytes("first"));

        List<LogEntry> entries = log.read(99, 10);

        assertTrue(entries.isEmpty());
    }

    @Test
    void restoresNextOffsetFromExistingLogFile() throws IOException {
        DiskMessageLog firstLog = new DiskMessageLog(tempDir.toString(), "payments", 0);
        firstLog.append(bytes("first"));
        firstLog.append(bytes("second"));

        DiskMessageLog reopenedLog = new DiskMessageLog(tempDir.toString(), "payments", 0);

        assertEquals(2, reopenedLog.size());
        assertEquals(2, reopenedLog.append(bytes("third")));
    }

    @Test
    void rejectsInvalidReadArguments() throws IOException {
        DiskMessageLog log = new DiskMessageLog(tempDir.toString(), "payments", 0);

        assertThrows(IllegalArgumentException.class, () -> log.read(-1, 10));
        assertThrows(IllegalArgumentException.class, () -> log.read(0, -1));
    }

    @Test
    void rejectsNullPayload() throws IOException {
        DiskMessageLog log = new DiskMessageLog(tempDir.toString(), "payments", 0);

        assertThrows(IllegalArgumentException.class, () -> log.append(null));
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private String text(LogEntry entry) {
        return new String(entry.getPayload(), StandardCharsets.UTF_8);
    }
}
