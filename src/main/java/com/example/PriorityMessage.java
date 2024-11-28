package com.example;

public class PriorityMessage extends Message{

    private int priority;
    private long timestamp;

    PriorityMessage(String msgBody) {
        super(msgBody);
    }

    PriorityMessage(String msgBody, int priority){
        super(msgBody);
        this.priority = priority;
    }

    PriorityMessage(String msgBody, int priority, long timeStamp){
        super(msgBody);
        this.priority = priority;
        this.timestamp = timeStamp;
    }

    PriorityMessage(String msgBody, String receiptId) {
        super(msgBody, receiptId);
    }

    PriorityMessage(String msgBody, String receiptId, int priority ) {
        super(msgBody, receiptId);
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
