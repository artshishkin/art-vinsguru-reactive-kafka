package net.shyshkin.study.kafkareactor.playground.sec03;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class Lec04KafkaProducer {

    private static Logger log = LoggerFactory.getLogger(Lec04KafkaProducer.class);

    public static void main(String[] args) {

        var producerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerProperties)
                .maxInFlight(10_000);

        var eventFlux = Flux.range(1, 1_000_000)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        LocalDateTime start = LocalDateTime.now();
        sender
                .send(eventFlux)
                .doOnNext(result -> log.info("Order {} was successfully published", result.correlationMetadata()))
                .doOnComplete(sender::close) // for one-time invocation
                .doOnComplete(() -> log.info("All events were published in {}", Duration.between(start, LocalDateTime.now())))
                .subscribe();


    }

}
