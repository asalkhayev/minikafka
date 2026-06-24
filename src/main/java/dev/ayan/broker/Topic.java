package dev.ayan.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Topic {
    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, int partitionCount, String dataDirectory) throws IOException {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }
        if (dataDirectory == null || dataDirectory.isBlank()) {
            throw new IllegalArgumentException("Data directory must not be empty");
        }

        this.name = name;

        List<Partition> createdPartitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            createdPartitions.add(new Partition(i, dataDirectory, name));
        }
        this.partitions = Collections.unmodifiableList(createdPartitions);
    }

    public Partition getPartition(int id) {
        if (id < 0 || id >= partitions.size()) {
            throw new IllegalArgumentException("Partition does not exist: " + id);
        }
        return partitions.get(id);
    }

    public String getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitions.size();
    }
}