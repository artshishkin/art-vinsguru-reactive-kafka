package net.shyshkin.study.kafkareactor.playground.sec12.exception;

public class SomeRetryableException extends RuntimeException{
    public SomeRetryableException(String message) {
        super(message);
    }
}
