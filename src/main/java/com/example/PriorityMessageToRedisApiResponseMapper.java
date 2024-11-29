package com.example;

public class PriorityMessageToRedisApiResponseMapper {
    public static String convertPriorityMessageToRedisApiResponse(PriorityMessage priorityMessage) {
        Message message= new Message(priorityMessage.getBody(), priorityMessage.getReceiptId());

        RedisMessageBody response = new RedisMessageBody(message, priorityMessage.getPriority(), priorityMessage.getTimestamp());
        response.getMessage().setAttempts(priorityMessage.getAttempts());
        response.getMessage().setVisibleFrom(priorityMessage.getVisibleFrom());
        String jsonResponse = "";
        try{
            jsonResponse = Utils.updateRedisMessageFields(response.toString());
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return jsonResponse;
    }
}
