package net.shyshkin.study.kafkareactor.playground.sec16;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerRunner implements CommandLineRunner {

    private final ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate;

    @Override
    public void run(String... args) throws Exception {
        consumerTemplate
                .receive()
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                .subscribe();
    }

}
