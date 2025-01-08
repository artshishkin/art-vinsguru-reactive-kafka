package net.shyshkin.study.kafkareactor.playground.sec11;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S11KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S11KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions = ReceiverOptions.<String, String>create(consumerProperties)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events")); //topic name

        int threadsCount = 10;

        KafkaReceiver<String, String> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        kafkaReceiver.receive()
                //.groupBy(r -> Integer.parseInt(r.key()) % threadsCount) //just for demo, use something like murmur2 algo similar to kafka
                .groupBy(r -> BuiltInPartitioner.partitionForKey(r.key().getBytes(StandardCharsets.UTF_8), threadsCount))
                //.groupBy(r -> r.key().hashCode() % threadsCount)
                // or groupBy r.partition()
                .flatMap(S11KafkaConsumer::batchProcess, threadsCount /*default 256*/)
                .subscribe();
    }

    private static Mono<Void> batchProcess(GroupedFlux<Integer, ReceiverRecord<String, String>> batch) {

        return batch
                .publishOn(Schedulers.boundedElastic())
                .doFirst(() -> log.info("-------------group: {}", batch.key()))
                .doOnNext(record -> {
                    log.info("key: {}, value: {}", record.key(), record.value());
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(10));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (!S11CheckOrderService.orderMatch(record.key(), record.value())) {
                        log.info("Order not match for {} : {}", record.key(), record.value());
                    }
                })
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .then();
    }

}
