package net.shyshkin.study.kafkareactor.playground.sec09;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S09KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S09KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1",
                MAX_POLL_RECORDS_CONFIG, 3 /* for demo, default = 500 */
        );
        var receiverOptions = ReceiverOptions.create(consumerProperties)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events")); //topic name

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receiveAutoAck()
                .log()
                .concatMap(S09KafkaConsumer::batchProcess)
                .subscribe();
    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> batch) {
        return batch
                .doFirst(() -> log.info("-------------Process batch-------------"))
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }

}
