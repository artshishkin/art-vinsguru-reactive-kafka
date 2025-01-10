package net.shyshkin.study.kafkareactor.playground.sec15;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;

public class TransferEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransferEventConsumer.class);

    private final KafkaReceiver<String, String> receiver;
    private final Mapper mapper;

    public TransferEventConsumer(KafkaReceiver<String, String> receiver, Mapper mapper) {
        this.receiver = receiver;
        this.mapper = mapper;
    }

    public Flux<TransferEvent> receive() {
        return receiver.receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .map(mapper::toTransferEvent);
    }

}
