package dev.ayan.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseIndexTest {

    @TempDir
    Path tempDir;

    @Test
    void returnsEmptyWhenIndexHasNoEntries() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);

        OptionalLong position = index.floorPosition(100);

        assertTrue(position.isEmpty());
        assertEquals(0, index.size());
    }

    @Test
    void returnsExactPositionForIndexedOffset() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);
        index.add(100, 4096);

        OptionalLong position = index.floorPosition(100);

        assertTrue(position.isPresent());
        assertEquals(4096, position.getAsLong());
    }

    @Test
    void returnsNearestFloorPositionForNonIndexedOffset() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);
        index.add(0, 0);
        index.add(100, 4096);
        index.add(200, 8192);

        OptionalLong position = index.floorPosition(150);

        assertTrue(position.isPresent());
        assertEquals(4096, position.getAsLong());
    }

    @Test
    void returnsEmptyWhenRequestedOffsetIsBeforeFirstIndexedOffset() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);
        index.add(100, 4096);

        OptionalLong position = index.floorPosition(50);

        assertTrue(position.isEmpty());
    }

    @Test
    void restoresIndexEntriesFromExistingFile() throws IOException {
        SparseIndex firstIndex = new SparseIndex(tempDir.toString(), "payments", 0);
        firstIndex.add(0, 0);
        firstIndex.add(100, 4096);

        SparseIndex reopenedIndex = new SparseIndex(tempDir.toString(), "payments", 0);

        assertEquals(2, reopenedIndex.size());
        OptionalLong position = reopenedIndex.floorPosition(150);
        assertTrue(position.isPresent());
        assertEquals(4096, position.getAsLong());
    }

    @Test
    void rejectsInvalidAddArguments() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);

        assertThrows(IllegalArgumentException.class, () -> index.add(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> index.add(0, -1));
    }

    @Test
    void rejectsInvalidFloorLookupArgument() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);

        assertThrows(IllegalArgumentException.class, () -> index.floorPosition(-1));
    }

    @Test
    void sizeCountsUniqueOffsets() throws IOException {
        SparseIndex index = new SparseIndex(tempDir.toString(), "payments", 0);
        index.add(100, 4096);
        index.add(100, 5000);

        assertEquals(1, index.size());
        OptionalLong position = index.floorPosition(100);
        assertTrue(position.isPresent());
        assertEquals(5000, position.getAsLong());
        assertFalse(index.floorPosition(99).isPresent());
    }
}
