package net.shyshkin.study.kafkareactor.playground.sec15;

public record TransferEvent(String key, String from, String to, Integer amount, Runnable acknowledge) {
}
