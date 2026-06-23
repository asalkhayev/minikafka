package dev.ayan;

import dev.ayan.broker.Topic;
import dev.ayan.client.Consumer;
import dev.ayan.client.Producer;

public class Main {
    public static void main(String[] args) throws Exception {
        // Messages will be stored in a "data" folder inside your project
        String dataDir = "data";

        Topic topic = new Topic("payments", 1, dataDir);
        Producer producer = new Producer(topic);
        Consumer consumer = new Consumer(topic, 0);

        producer.send("payment:user1:50USD", 0);
        producer.send("payment:user2:30USD", 0);
        producer.send("payment:user3:100USD", 0);

        System.out.println("\n--- Consumer polls first 2 ---");
        consumer.poll(2);

        System.out.println("\n--- Producer sends more ---");
        producer.send("payment:user4:20USD", 0);
        producer.send("payment:user5:75USD", 0);

        System.out.println("\n--- Consumer polls again (continues from offset 2) ---");
        consumer.poll(10);
    }
}