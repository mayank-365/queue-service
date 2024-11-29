package com.example.dto;

import com.example.Message;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PriorityMessage extends Message {

    private int priority;
    private long timestamp;

    public PriorityMessage(String msgBody, int score, long timestamp) {
        super(msgBody);
        this.priority = score;
        this.timestamp = timestamp;
    }

    public PriorityMessage(String msgBody, String receiptId, int priority, long timestamp,
        int attempts, long visibleFrom) {
        super(msgBody, receiptId);
        super.setAttempts(attempts);
        super.setVisibleFrom(visibleFrom);
        this.priority = priority;
        this.timestamp = timestamp;
    }
}
