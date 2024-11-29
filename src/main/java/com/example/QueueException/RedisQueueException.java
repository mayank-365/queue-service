package com.example.QueueException;

public class RedisQueueException extends RuntimeException {
    private final String messageCode;
    private final String messageDesc;

    public RedisQueueException(String messageCode, String messageDesc) {
        this.messageCode = messageCode;
        this.messageDesc = messageDesc;
    }
}
