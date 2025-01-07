package net.shyshkin.study.kafkareactor.playground.sec02;

import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class Lec03KafkaProducer {

    public static void main(String[] args) {

        var producerProperties = Map.<String, Object>of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.create(producerProperties);
//        KafkaSender.create(senderOptions)


    }

}
