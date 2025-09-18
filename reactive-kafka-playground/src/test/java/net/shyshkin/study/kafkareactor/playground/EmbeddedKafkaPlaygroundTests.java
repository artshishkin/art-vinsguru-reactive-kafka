package net.shyshkin.study.kafkareactor.playground;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@EmbeddedKafka(ports = 9092)
class EmbeddedKafkaPlaygroundTests {

    @Test
    void embeddedKafkaDemo() {

        StepVerifier.create(Producer.run())
                .verifyComplete();

        StepVerifier.create(Consumer.run())
                .verifyComplete();
    }

    private static class Consumer {

        private static final Logger log = LoggerFactory.getLogger(Consumer.class);

        public static Mono<Void> run() {
            var consumerProperties = Map.<String, Object>of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
            );
            var receiverOptions = ReceiverOptions.create(consumerProperties)
                    .subscription(List.of("order-events")); //topic name

            KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
            return kafkaReceiver.receive()
                    .take(10)
                    .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                    .doOnNext(record -> record.receiverOffset().acknowledge())
                    .then();
        }
    }

    private static class Producer {
        private static final Logger log = LoggerFactory.getLogger(Producer.class);

        public static Mono<Void> run() {

            var producerProperties = Map.<String, Object>of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
            );

            var senderOptions = SenderOptions.<String, String>create(producerProperties);

            var eventFlux = Flux.range(1,10)
                    .delayElements(Duration.ofMillis(10))
                    .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                    .map(pr -> SenderRecord.create(pr, pr.key()));

            KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
            return sender
                    .send(eventFlux)
                    .doOnNext(result -> log.info("Order {} was successfully published", result.correlationMetadata()))
                    .doOnComplete(sender::close) // for one-time invocation
                    .then();
        }
    }

}
