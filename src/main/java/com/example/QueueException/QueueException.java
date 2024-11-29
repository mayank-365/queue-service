package com.example.QueueException;

import lombok.Getter;

@Getter
public class QueueException extends RuntimeException {
    private final String messageCode;
    private final String messageDesc;

    public QueueException(String messageCode, String messageDesc) {
        super(messageDesc);
        this.messageCode = messageCode;
        this.messageDesc = messageDesc;
    }

}
