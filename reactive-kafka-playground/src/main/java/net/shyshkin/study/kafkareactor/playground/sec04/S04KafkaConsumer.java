package net.shyshkin.study.kafkareactor.playground.sec04;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S04KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S04KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "inventory-service-group",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var receiverOptions = ReceiverOptions.create(consumerProperties)
//                .subscription(List.of("order-events", "order-returns", "order-must3")); //list of topic names
                .subscription(Pattern.compile("order-.*")); //pattern of topic names - starts with `order-`

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receive()
                .doOnNext(record -> log.info("topic: {}, key: {}, value: {}", record.topic(), record.key(), record.value()))
                .doOnNext(record -> record.headers().forEach(h->log.info("header key: {}, value: {}", h.key(), new String(h.value()))))
                .doOnNext(record -> record.receiverOffset().acknowledge())
                .subscribe();
    }

}
