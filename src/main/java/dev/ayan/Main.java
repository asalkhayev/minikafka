package dev.ayan;

import dev.ayan.client.GroupConsumer;
import dev.ayan.client.Producer;
import dev.ayan.server.BrokerServer;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        // Start each demo run with a clean data directory.
        // Real brokers keep old log files; this reset is only for the demo app.
        clearDirectory(new File("data"));

        // Start broker in background thread
        BrokerServer broker = new BrokerServer(9092, "data");
        broker.createTopic("payments", 1);
        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "mini-kafka-broker");
        brokerThread.setDaemon(true);
        brokerThread.start();

        // Give broker a moment to start
        Thread.sleep(500);

        // Producer and group consumers communicate with the broker over TCP
        try (Producer producer = new Producer("localhost", 9092)) {
            producer.send("payments", "payment:user1:50USD", 0);
            producer.send("payments", "payment:user2:30USD", 0);
            producer.send("payments", "payment:user3:100USD", 0);
            producer.send("payments", "payment:user4:20USD", 0);
            producer.send("payments", "payment:user5:75USD", 0);
        }

        System.out.println("\n--- Group consumer instance 1: poll first 2 and commit ---");
        try (GroupConsumer consumer1 = new GroupConsumer("localhost", 9092, "payments-service", "payments", 0)) {
            consumer1.poll(2);
            consumer1.commit();
        }

        System.out.println("\n--- Group consumer instance 2: resumes from committed offset ---");
        try (GroupConsumer consumer2 = new GroupConsumer("localhost", 9092, "payments-service", "payments", 0)) {
            consumer2.poll(10);
            consumer2.commit();
        }

        System.out.println("\n--- Different group: starts from offset 0 ---");
        try (GroupConsumer auditConsumer = new GroupConsumer("localhost", 9092, "audit-service", "payments", 0)) {
            auditConsumer.poll(10);
            auditConsumer.commit();
        }

        broker.stop();
        brokerThread.join(1000);
    }
    private static void clearDirectory(File directory) {
        if (!directory.exists()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                clearDirectory(file);
            }
            if (!file.delete()) {
                throw new IllegalStateException("Could not delete demo file: " + file.getAbsolutePath());
            }
        }
    }
}