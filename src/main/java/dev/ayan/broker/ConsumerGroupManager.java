
package dev.ayan.broker;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupManager {

    private final Map<OffsetKey, Long> committedOffsets = new ConcurrentHashMap<>();

    public void commitOffset(String groupId, String topicName, int partitionId, long offset) {
        validate(groupId, topicName, partitionId);
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }

        committedOffsets.put(new OffsetKey(groupId, topicName, partitionId), offset);
        System.out.println("[ConsumerGroupManager] Committed offset " + offset +
                " for group=" + groupId +
                ", topic=" + topicName +
                ", partition=" + partitionId);
    }

    public long getCommittedOffset(String groupId, String topicName, int partitionId) {
        validate(groupId, topicName, partitionId);
        return committedOffsets.getOrDefault(new OffsetKey(groupId, topicName, partitionId), 0L);
    }

    private void validate(String groupId, String topicName, int partitionId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group id must not be empty");
        }
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id must not be negative");
        }
    }

    private record OffsetKey(String groupId, String topicName, int partitionId) {
        private OffsetKey {
            Objects.requireNonNull(groupId, "groupId");
            Objects.requireNonNull(topicName, "topicName");
        }
    }
}
