package dev.ayan;

import dev.ayan.client.NetworkConsumer;
import dev.ayan.client.NetworkProducer;
import dev.ayan.server.BrokerServer;

public class Main {
    public static void main(String[] args) throws Exception {
        // Start broker in background thread
        BrokerServer broker = new BrokerServer(9092, "data");
        broker.createTopic("payments", 1);
        Thread brokerThread = new Thread(() -> {
            try { broker.start(); }
            catch (Exception e) { e.printStackTrace(); }
        });
        brokerThread.setDaemon(true);
        brokerThread.start();

        // Give broker a moment to start
        Thread.sleep(500);

        // Producer sends over TCP
        NetworkProducer producer = new NetworkProducer("localhost", 9092);
        producer.send("payments", 0, "payment:user1:50USD");
        producer.send("payments", 0, "payment:user2:30USD");
        producer.send("payments", 0, "payment:user3:100USD");

        // Consumer fetches over TCP
        NetworkConsumer consumer = new NetworkConsumer("localhost", 9092, "payments", 0);
        System.out.println("\n--- Poll 1 ---");
        consumer.poll(2);

        producer.send("payments", 0, "payment:user4:20USD");
        producer.send("payments", 0, "payment:user5:75USD");

        System.out.println("\n--- Poll 2 ---");
        consumer.poll(10);
    }
}