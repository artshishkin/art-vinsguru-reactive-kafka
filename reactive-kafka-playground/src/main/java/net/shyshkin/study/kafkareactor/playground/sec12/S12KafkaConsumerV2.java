package net.shyshkin.study.kafkareactor.playground.sec12;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class S12KafkaConsumerV2 {

    private static final Logger log = LoggerFactory.getLogger(S12KafkaConsumerV2.class);

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
                .concatMap(S12KafkaConsumerV2::process)
                .subscribe();
    }

    private static Mono<Void> process(ReceiverRecord<Object, Object> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    int errorProbability = ThreadLocalRandom.current().nextInt(0, 100);
                    if (errorProbability > 33) {
                        throw new RuntimeException("Sorry. Something bad happened during processing " + r.value().toString());
                    }
                    log.info("key: {}, value: {}", record.key(), record.value());
//                    r.receiverOffset().acknowledge();
                })
                .doOnError(ex -> log.error(ex.getMessage()))
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1)))
                .doOnError(ex -> log.error(ex.getMessage()))
                .doFinally(s -> record.receiverOffset().acknowledge()) //ack anyway - for demo
                .onErrorComplete()
                .then();
    }

}
