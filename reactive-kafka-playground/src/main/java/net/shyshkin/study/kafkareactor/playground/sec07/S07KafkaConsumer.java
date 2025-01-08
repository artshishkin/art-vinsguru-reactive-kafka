package net.shyshkin.study.kafkareactor.playground.sec07;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S07KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S07KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group-123",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions = ReceiverOptions.create(consumerProperties)
                .addAssignListener(c -> {
                            c.forEach(partition -> log.info("topic: {}, partition: {}, topicPartition.toString(): {}, position: {}",
                                    partition.topicPartition().topic(),
                                    partition.topicPartition().partition(),
                                    partition.topicPartition(),
                                    partition.position()));
//                            c.forEach(partition -> partition.seek(2000));
//                            c.forEach(partition -> partition.seek(partition.position() - 10)); //last 10 items
//                            c.forEach(ReceiverPartition::seekToBeginning); //replay all
                            c.forEach(partition-> partition.seekToTimestamp(LocalDateTime.parse("2025-01-07T21:22:15").toInstant(ZoneOffset.UTC).toEpochMilli()));
                        }
                )
                .subscription(List.of("order-events")); //topic name

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receive()
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                .doOnNext(record -> record.receiverOffset().acknowledge())
                .subscribe();
    }

}
