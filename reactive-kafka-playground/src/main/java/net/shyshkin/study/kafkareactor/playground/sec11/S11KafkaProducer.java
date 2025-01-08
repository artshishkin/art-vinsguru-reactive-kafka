package net.shyshkin.study.kafkareactor.playground.sec11;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class S11KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(S11KafkaProducer.class);

    public static void main(String[] args) {

        var producerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerProperties);

        var eventFlux = Flux.range(1, 10_000)
                .map(i -> new ProducerRecord<>("order-events", String.valueOf(i % 20), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        sender
                .send(eventFlux)
                .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();


    }

}
