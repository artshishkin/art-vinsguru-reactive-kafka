package net.shyshkin.study.kafkareactor.playground.sec14;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S14KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S14KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                GROUP_ID_CONFIG, "demo-group",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions = ReceiverOptions.<String, Integer>create(consumerProperties)
                .withValueDeserializer(errorHandlingDeserializer())
                .subscription(List.of("order-events")); //topic name

        KafkaReceiver<String, Integer> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receive()
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                .doOnNext(record -> record.receiverOffset().acknowledge())
                .subscribe();
    }

    private static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer() {
        ErrorHandlingDeserializer<Integer> deserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
        deserializer.setFailedDeserializationFunction(info -> {
            log.error("failed record: {} in topic `{}`", new String(info.getData()), info.getTopic());
            return -10_000;
        });
        return deserializer;
    }

}
