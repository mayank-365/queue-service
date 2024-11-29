package com.example.dto;

import com.example.Message;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public class RedisMessageBody {

    private Message message;
    private int score;
    private long timestamp;

    @JsonCreator
    public RedisMessageBody(@JsonProperty("message") Message message,
        @JsonProperty("score") int score, @JsonProperty("timestamp") long timestamp) {
        this.message = new Message(message.getMsgBody(), message.getReceiptId());
        this.message.setAttempts(message.getAttempts());
        this.message.setVisibleFrom(message.getVisibleFrom());
        this.timestamp = timestamp;
        this.score = score;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String safeMsgBody = objectMapper.writeValueAsString(this.getMessage().getBody());

            return "{" + "\"message\": {" + "\"msgBody\": " + safeMsgBody + ", " + "\"attempts\": "
                + this.getMessage().getAttempts() + ", " + "\"visibleFrom\": " + this.getMessage()
                .getVisibleFrom() + ", " + "\"receiptId\": \"" + this.getMessage().getReceiptId()
                + "\" " + "}, " + "\"score\": " + score + ", " + "\"timestamp\": " + timestamp
                + "}";
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize msgBody", e);
        }
    }


}
