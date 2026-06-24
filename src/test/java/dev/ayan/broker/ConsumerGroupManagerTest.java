package dev.ayan.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumerGroupManagerTest {

    @Test
    void returnsZeroWhenGroupHasNoCommittedOffset() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        long offset = manager.getCommittedOffset("payments-service", "payments", 0);

        assertEquals(0, offset);
    }

    @Test
    void returnsCommittedOffsetForGroupTopicAndPartition() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        manager.commitOffset("payments-service", "payments", 0, 42);

        assertEquals(42, manager.getCommittedOffset("payments-service", "payments", 0));
    }

    @Test
    void keepsDifferentGroupsSeparate() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        manager.commitOffset("payments-service", "payments", 0, 10);
        manager.commitOffset("audit-service", "payments", 0, 99);

        assertEquals(10, manager.getCommittedOffset("payments-service", "payments", 0));
        assertEquals(99, manager.getCommittedOffset("audit-service", "payments", 0));
    }

    @Test
    void keepsDifferentTopicsSeparate() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        manager.commitOffset("payments-service", "payments", 0, 10);
        manager.commitOffset("payments-service", "orders", 0, 25);

        assertEquals(10, manager.getCommittedOffset("payments-service", "payments", 0));
        assertEquals(25, manager.getCommittedOffset("payments-service", "orders", 0));
    }

    @Test
    void keepsDifferentPartitionsSeparate() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        manager.commitOffset("payments-service", "payments", 0, 10);
        manager.commitOffset("payments-service", "payments", 1, 50);

        assertEquals(10, manager.getCommittedOffset("payments-service", "payments", 0));
        assertEquals(50, manager.getCommittedOffset("payments-service", "payments", 1));
    }

    @Test
    void overwritesPreviousCommitForSameKey() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        manager.commitOffset("payments-service", "payments", 0, 10);
        manager.commitOffset("payments-service", "payments", 0, 20);

        assertEquals(20, manager.getCommittedOffset("payments-service", "payments", 0));
    }

    @Test
    void rejectsInvalidCommitArguments() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        assertThrows(IllegalArgumentException.class, () -> manager.commitOffset(null, "payments", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> manager.commitOffset("", "payments", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> manager.commitOffset("payments-service", null, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> manager.commitOffset("payments-service", "", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> manager.commitOffset("payments-service", "payments", -1, 1));
        assertThrows(IllegalArgumentException.class, () -> manager.commitOffset("payments-service", "payments", 0, -1));
    }

    @Test
    void rejectsInvalidGetArguments() {
        ConsumerGroupManager manager = new ConsumerGroupManager();

        assertThrows(IllegalArgumentException.class, () -> manager.getCommittedOffset(null, "payments", 0));
        assertThrows(IllegalArgumentException.class, () -> manager.getCommittedOffset("", "payments", 0));
        assertThrows(IllegalArgumentException.class, () -> manager.getCommittedOffset("payments-service", null, 0));
        assertThrows(IllegalArgumentException.class, () -> manager.getCommittedOffset("payments-service", "", 0));
        assertThrows(IllegalArgumentException.class, () -> manager.getCommittedOffset("payments-service", "payments", -1));
    }
}
