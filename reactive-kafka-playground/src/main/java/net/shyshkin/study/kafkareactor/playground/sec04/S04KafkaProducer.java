package net.shyshkin.study.kafkareactor.playground.sec04;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
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

public class S04KafkaProducer {

    private static Logger log = LoggerFactory.getLogger(S04KafkaProducer.class);

    public static void main(String[] args) {

        var producerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerProperties);

        var eventFlux = Flux.range(1, 10)
                .map(S04KafkaProducer::createSenderRecord);

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        LocalDateTime start = LocalDateTime.now();
        sender
                .send(eventFlux)
                .doOnNext(result -> log.info("Order {} was successfully published", result.correlationMetadata()))
                .doOnComplete(sender::close) // for one-time invocation
                .doOnComplete(() -> log.info("All events were published in {}", Duration.between(start, LocalDateTime.now())))
                .subscribe();


    }

    private static SenderRecord<String, String, Object> createSenderRecord(Integer i) {
        // Solution 1
        var headers = new RecordHeaders();
        headers
                .add("X-Art", "my header value".getBytes())
                .add("client-id", "some client".getBytes())
                .add("tracing-id", "123".getBytes())
        ;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, headers);

        // Solution 2
        //ProducerRecord<String, String> producerRecord = new ProducerRecord<>("order-events", i.toString(), "order-" + i);
        //producerRecord.headers()
        //        .add("X-Art", "my header value".getBytes())
        //        .add("client-id", "some client".getBytes())
        //        .add("tracing-id", "123".getBytes())
        //;

        return SenderRecord.create(producerRecord, producerRecord.key());
    }

}
