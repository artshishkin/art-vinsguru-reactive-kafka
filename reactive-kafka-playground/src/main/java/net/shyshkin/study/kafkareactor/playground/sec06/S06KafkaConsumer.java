package net.shyshkin.study.kafkareactor.playground.sec06;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S06KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S06KafkaConsumer.class);

    public static void start(String instanceId) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group-123",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, instanceId,
                PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()
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
