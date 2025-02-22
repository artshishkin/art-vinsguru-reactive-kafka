package net.shyshkin.study.kafkareactor.playground.sec05;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S05KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S05KafkaConsumer.class);

    public static void start(String instanceId) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group-123",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, instanceId
        );
        var receiverOptions = ReceiverOptions.create(consumerProperties)
                .subscription(List.of("order-events")); //topic name

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receive()
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                .doOnNext(record -> record.receiverOffset().acknowledge())
                .subscribe();
    }

}
