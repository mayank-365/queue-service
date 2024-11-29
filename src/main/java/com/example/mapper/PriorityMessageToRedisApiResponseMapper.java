package com.example.mapper;

import com.example.Message;
import com.example.dto.PriorityMessage;
import com.example.utils.Utils;
import com.example.dto.RedisMessageBody;

public class PriorityMessageToRedisApiResponseMapper {

    public static String convertPriorityMessageToRedisApiResponse(PriorityMessage priorityMessage) {
        Message message = new Message(priorityMessage.getBody(), priorityMessage.getReceiptId());

        RedisMessageBody response = new RedisMessageBody(message, priorityMessage.getPriority(),
            priorityMessage.getTimestamp());
        response.getMessage().setAttempts(priorityMessage.getAttempts());
        response.getMessage().setVisibleFrom(priorityMessage.getVisibleFrom());
        String jsonResponse = "";
        try {
            jsonResponse = Utils.updateRedisMessageFields(response.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return jsonResponse;
    }
}
