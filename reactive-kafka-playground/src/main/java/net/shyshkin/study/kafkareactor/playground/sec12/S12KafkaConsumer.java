package net.shyshkin.study.kafkareactor.playground.sec12;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S12KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S12KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions = ReceiverOptions.create(consumerProperties)
                .subscription(List.of("order-events")); //topic name

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receive()
                .log()
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value().toString().charAt(15))) //should throw exception
                .doOnError(ex -> log.error(ex.getMessage()))
                .doOnNext(record -> record.receiverOffset().acknowledge())
                .subscribe();
    }

}
