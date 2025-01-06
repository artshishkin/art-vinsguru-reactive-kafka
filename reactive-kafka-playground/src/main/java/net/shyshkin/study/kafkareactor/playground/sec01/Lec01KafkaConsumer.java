package net.shyshkin.study.kafkareactor.playground.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.listener.ConsumerProperties;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class Lec01KafkaConsumer {

    public static void main(String[] args) {
        Map<String, Object> consumerProperties = Map.of(
                BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                GROUP_ID_CONFIG, "demo-group"
        );
//        var receiverOptions = ReceiverOptions.create(consumerProperties);

//        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
    }

}
