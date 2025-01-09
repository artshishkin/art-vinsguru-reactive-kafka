package net.shyshkin.study.kafkareactor.playground.sec13;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class S13KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(S13KafkaProducer.class);

    public static void main(String[] args) {

        var producerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerProperties);

        var eventFlux = Flux.interval(Duration.ofMillis(50))
                .take(100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        sender
                .send(eventFlux)
                .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();


    }

}
