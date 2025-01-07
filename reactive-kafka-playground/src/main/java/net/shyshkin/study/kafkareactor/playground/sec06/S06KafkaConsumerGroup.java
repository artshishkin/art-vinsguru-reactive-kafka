package net.shyshkin.study.kafkareactor.playground.sec06;

public class S06KafkaConsumerGroup {

    //kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --partitions 3

    private static class Consumer1 {
        public static void main(String[] args) {
            S06KafkaConsumer.start("1");
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            S06KafkaConsumer.start("2");
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            S06KafkaConsumer.start("3");
        }
    }

}
