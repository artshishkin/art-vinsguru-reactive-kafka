package net.shyshkin.study.kafkareactor.playground.sec15;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TransferDemo {

    private static final Logger log = LoggerFactory.getLogger(TransferDemo.class);

    public static void main(String[] args) {

        Mapper mapper = new Mapper();
        TransferEventProcessor transferEventProcessor = new TransferEventProcessor(kafkaSender(), mapper);
        TransferEventConsumer transferEventConsumer = new TransferEventConsumer(kafkaReceiver(), mapper);

        transferEventConsumer.receive()
                .transform(transferEventProcessor::process)
                .doOnNext(r -> log.info("transaction result: {}", r))
                .doOnError(ex -> log.error(ex.getMessage()))
                .subscribe();
    }

    private static KafkaReceiver<String, String> kafkaReceiver() {
        var consumerProperties = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group",
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions = ReceiverOptions.<String, String>create(consumerProperties)
                .subscription(List.of("transfer-requests")); //topic name

        return KafkaReceiver.create(receiverOptions);
    }

    private static KafkaSender<String, String> kafkaSender() {
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                TRANSACTIONAL_ID_CONFIG, "money-transfer"
        );

        var senderOptions = SenderOptions.<String, String>create(producerProperties);

        return KafkaSender.create(senderOptions);
    }


}
