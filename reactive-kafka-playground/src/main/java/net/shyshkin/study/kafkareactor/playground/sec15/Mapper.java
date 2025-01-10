package net.shyshkin.study.kafkareactor.playground.sec15;

import reactor.kafka.receiver.ReceiverRecord;

public class Mapper {

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

    private Runnable ack(ReceiverRecord<String, String> record) {
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail(ReceiverRecord<String, String> record) {
        return () -> {
            throw new RuntimeException("error while ack");
        };
    }

}
