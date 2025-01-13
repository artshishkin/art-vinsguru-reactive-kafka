package net.shyshkin.study.kafkareactor.playground.sec16;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate(KafkaProperties properties) {
        SenderOptions<String, OrderEvent> options = SenderOptions.create(properties.buildProducerProperties());
        return new ReactiveKafkaProducerTemplate<>(options);
    }

}
