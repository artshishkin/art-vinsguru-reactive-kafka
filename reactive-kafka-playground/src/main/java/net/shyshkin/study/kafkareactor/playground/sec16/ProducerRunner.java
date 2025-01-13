package net.shyshkin.study.kafkareactor.playground.sec16;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerRunner implements CommandLineRunner {

    private final ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate;

    @Override
    public void run(String... args) throws Exception {
        this.orderFlux()
                .doOnNext(event -> log.info("sending: {}", event))
                .flatMap(event -> producerTemplate.send("order-events", String.valueOf(event.customerId()), event))
                .doOnNext(result -> log.info("recordMetadata: {}", result.recordMetadata()))
                .subscribe();
    }

    private Flux<OrderEvent> orderFlux() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(i -> new OrderEvent(UUID.randomUUID(), i % 5, LocalDateTime.now()));
    }

}
