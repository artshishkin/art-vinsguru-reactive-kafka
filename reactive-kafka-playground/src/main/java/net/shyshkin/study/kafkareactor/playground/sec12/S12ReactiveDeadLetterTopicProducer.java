package net.shyshkin.study.kafkareactor.playground.sec12;

import net.shyshkin.study.kafkareactor.playground.sec12.exception.RecordProcessingException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

public class S12ReactiveDeadLetterTopicProducer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(S12ReactiveDeadLetterTopicProducer.class);
    private final KafkaSender<K, V> sender;
    private final Retry retrySpec;

    public S12ReactiveDeadLetterTopicProducer(KafkaSender<K, V> sender, Retry retrySpec) {
        this.retrySpec = retrySpec;
        this.sender = sender;
    }

    public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> record) {
        SenderRecord<K, V, K> senderRecord = toSenderRecord(record);
        return sender.send(Mono.just(senderRecord))
                .next();
    }

    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> rr) {
        ProducerRecord<K, V> pr = new ProducerRecord<>(
                rr.topic() + "-dlt",
                null,
                rr.key(),
                rr.value(),
                rr.headers()
        );
        return SenderRecord.create(pr, pr.key());
    }

    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono -> mono
                .retryWhen(retrySpec)
                .onErrorMap(ex -> ex.getCause() instanceof RecordProcessingException, Throwable::getCause)
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(
                        RecordProcessingException.class,
                        ex -> this.produce(ex.getRecord())
                                .then(Mono.fromRunnable(() -> ex.getRecord().receiverOffset().acknowledge()))
                )
                .then();
    }

}
