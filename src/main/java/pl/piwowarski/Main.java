package pl.piwowarski;

import pl.piwowarski.consumer.SimpleConsumer;
import pl.piwowarski.producer.SimpleProducer;

import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "demo-topic");

        if (args.length == 0) {
            System.out.println("Please pass either 'producer' or 'consumer' as the first argument.");
            return;
        }

        switch (args[0]) {
            case "producer":
                try (SimpleProducer p = new SimpleProducer(bootstrap, topic)) {
                    int i = 1;
                    while (true) {
                        p.sendAsync("key-" + i, "hello-kafka-" + i);
                        System.out.println("Sent message " + i);
                        i++;
                        Thread.sleep(500);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "consumer":
                String consumerName = args.length > 1 ? args[1] : "consumer-default";
                try (SimpleConsumer c = new SimpleConsumer(bootstrap, topic, "demo-group", consumerName)) {
                    for (int i = 0; i < 20; i++) {
                        c.pollAndPrint();
                    }
                }
                break;
            default:
                System.out.println("Unknown mode: " + args[0]);
        }
    }
}
