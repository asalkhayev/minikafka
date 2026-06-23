package dev.ayan.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Topic {
    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, int partitionCount, String dataDirectory) throws IOException {
        this.name = name;
        this.partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new Partition(i, dataDirectory, name));
        }
    }

    public Partition getPartition(int id) { return partitions.get(id); }
    public String getName() { return name; }
    public int getPartitionCount() { return partitions.size(); }
}