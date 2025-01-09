package net.shyshkin.study.kafkareactor.playground.sec12;

import net.shyshkin.study.kafkareactor.playground.sec12.exception.RecordProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.concurrent.ThreadLocalRandom;

public class OrderEventProcessor {
    private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);
    private final S12ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;

    public OrderEventProcessor(S12ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer) {
        this.deadLetterTopicProducer = deadLetterTopicProducer;
    }

    public Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    int errorProbability = ThreadLocalRandom.current().nextInt(0, 100);
                    if (errorProbability > 50) {
                        throw new RuntimeException("Something bad happened during processing " + r.value() + ". You can retry.");
                    }
                    log.info("key: {}, value: {}", record.key(), record.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingException(record, ex))
                .transform(deadLetterTopicProducer.recordProcessingErrorHandler());
    }

}
