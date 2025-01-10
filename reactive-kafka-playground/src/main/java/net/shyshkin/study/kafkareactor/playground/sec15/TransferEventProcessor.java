package net.shyshkin.study.kafkareactor.playground.sec15;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;

import java.time.Duration;
import java.util.function.Predicate;

public class TransferEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransferEventProcessor.class);

    private final KafkaSender<String, String> sender;
    private final Mapper mapper;

    public TransferEventProcessor(KafkaSender<String, String> sender, Mapper mapper) {
        this.sender = sender;
        this.mapper = mapper;
    }

    public Flux<SenderResult<String>> process(Flux<TransferEvent> events) {
        return events
                .concatMap(this::validate)
                .concatMap(this::sendTransaction);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent event) {
        var senderRecords = mapper.toSenderRecords(event);
        var recordFlux = Flux.fromIterable(senderRecords);
        TransactionManager manager = this.sender.transactionManager();

        return manager.begin()
                .then(sender.send(recordFlux)
                        .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.acknowledge())))
                        .concatWith(manager.commit())
                        .last() //for simplicity. It is recommended to .collectList -> analyze -> make own consolidated result
                )
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> manager.abort());
    }

    // simulate situation
    // if key=5 account does not have enough money
    private Mono<TransferEvent> validate(TransferEvent event) {
        return Mono.just(event)
                .filter(Predicate.not(e -> "5".equals(e.key())))
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(event.acknowledge())
                                .doFirst(() -> log.info("fails validation: {}", event))
                );
    }

}
