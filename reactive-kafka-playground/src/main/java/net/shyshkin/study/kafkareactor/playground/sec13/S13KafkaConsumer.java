package net.shyshkin.study.kafkareactor.playground.sec13;

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
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class S13KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(S13KafkaConsumer.class);

    public static void main(String[] args) {
        KafkaReceiver<String, String> kafkaReceiver = kafkaReceiver();

        OrderEventProcessor orderEventProcessor = new OrderEventProcessor(deadLetterTopicProducer());

        kafkaReceiver.receive()
                .concatMap(orderEventProcessor::process)
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
                .subscription(List.of("order-events", "order-events-dlt")); //topic name

        return KafkaReceiver.create(receiverOptions);
    }

    private static S13ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer() {
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerProperties);

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        return new S13ReactiveDeadLetterTopicProducer<>(sender, retrySpec());
    }

    private static Retry retrySpec() {
        return Retry.fixedDelay(2, Duration.ofSeconds(1));
    }


}
