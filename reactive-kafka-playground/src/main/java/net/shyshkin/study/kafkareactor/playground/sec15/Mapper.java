package net.shyshkin.study.kafkareactor.playground.sec15;

import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderRecord;

import java.util.List;
import java.util.stream.Stream;

public class Mapper {

    public static final String TOPIC = "transaction-events";

    //1:a,b,100
    //key:from,to,amount
    public TransferEvent toTransferEvent(ReceiverRecord<String, String> record) {
        String[] arr = record.value().split(",");
        if (arr.length != 3)
            throw new IllegalArgumentException("Event message wrong: " + record.value());

        //simulating error during ack
        Runnable action = record.key().equals("6") ?
                fail(record) :
                ack(record);

        return new TransferEvent(
                record.key(),
                arr[0],
                arr[1],
                Integer.parseInt(arr[2]),
                action
        );
    }

    public List<SenderRecord<String, String, String>> toSenderRecords(TransferEvent event) {
        return Stream.of(
                        new ProducerRecord<>(TOPIC, event.key(), "%s,%d".formatted(event.from(), -event.amount())),
                        new ProducerRecord<>(TOPIC, event.key(), "%s,%d".formatted(event.to(), event.amount()))
                )
                .map(pr -> SenderRecord.create(pr, pr.key()))
                .toList();
    }

    private Runnable ack(ReceiverRecord<String, String> record) {
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail(ReceiverRecord<String, String> record) {
        return () -> {
            throw new RuntimeException("error while ack");
        };
    }

}
