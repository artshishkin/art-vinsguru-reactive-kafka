package net.shyshkin.study.kafkareactor.playground.sec12.exception;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {

    private final ReceiverRecord<?, ?> record;

    public RecordProcessingException(ReceiverRecord<?, ?> record, Throwable cause) {
        super(cause);
        this.record = record;
    }

    @SuppressWarnings("unchecked")
    public <K, V> ReceiverRecord<K, V> getRecord() {
        return (ReceiverRecord<K, V>) record;
    }
}
