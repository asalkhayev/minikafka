package dev.ayan.benchmark;

import dev.ayan.client.Producer;
import dev.ayan.server.BrokerServer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ProducerBenchmark {

    private static final int PORT = 9093;
    private static final String DATA_DIR = "benchmark-data";
    private static final String TOPIC = "benchmark-topic";
    private static final int PARTITION = 0;
    private static final int MESSAGE_COUNT = 10_000;
    private static final int PAYLOAD_SIZE_BYTES = 100;

    public static void main(String[] args) throws Exception {
        clearDirectory(new File(DATA_DIR));

        BrokerServer broker = new BrokerServer(PORT, DATA_DIR);
        broker.createTopic(TOPIC, 1);

        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "producer-benchmark-broker");
        brokerThread.start();

        Thread.sleep(500);

        byte[] payload = createPayload(PAYLOAD_SIZE_BYTES);

        long startNanos = System.nanoTime();
        try (Producer producer = new Producer("localhost", PORT, false)) {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                producer.send(TOPIC, payload, PARTITION);
            }
        }
        long endNanos = System.nanoTime();

        double elapsedSeconds = (endNanos - startNanos) / 1_000_000_000.0;
        double throughput = MESSAGE_COUNT / elapsedSeconds;
        double averageLatencyMillis = (elapsedSeconds * 1000.0) / MESSAGE_COUNT;

        System.out.println();
        System.out.println("=== Producer Benchmark ===");
        System.out.println("Messages: " + MESSAGE_COUNT);
        System.out.println("Payload size: " + PAYLOAD_SIZE_BYTES + " bytes");
        System.out.printf("Elapsed time: %.3f seconds%n", elapsedSeconds);
        System.out.printf("Throughput: %.2f messages/second%n", throughput);
        System.out.printf("Average latency: %.4f ms/message%n", averageLatencyMillis);

        broker.stop();
        brokerThread.join(1000);
    }

    private static byte[] createPayload(int sizeBytes) {
        byte[] payload = new byte[sizeBytes];
        Arrays.fill(payload, (byte) 'x');
        return new String(payload, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
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
                throw new IllegalStateException("Could not delete benchmark file: " + file.getAbsolutePath());
            }
        }
    }
}
