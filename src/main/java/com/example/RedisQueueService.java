package com.example;

import com.example.apiresponse.RedisApiResponse;
import com.example.constants.Constants;
import com.example.mapper.PriorityMessageToRedisApiResponseMapper;
import com.example.dto.RedisMessageBody;
import com.example.utils.Utils;
import com.example.dto.PriorityMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisQueueService implements QueueService {


    @Override
    public void push(String queueUrl, String msgBody) {
        try {
            msgBody = Utils.updateRedisMessageFields(msgBody);
            makePostRequest(
                Constants.REDIS_CONF.getRedisEndpointUrl() + "/" + Constants.REDIS_ZADD_COMMAND
                    + "/" + queueUrl + "/" + extractPriorityFromJson(msgBody), msgBody);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public PriorityMessage pull(String queueUrl) {

        String redisResponse = makeGetRequest(
            Constants.REDIS_CONF.getRedisEndpointUrl() + "/" + Constants.REDIS_REV_RANGE_COMMAND
                + "/" + queueUrl + "/0/-1");
        List<PriorityMessage> messages = createMessageFromRedisResponse(redisResponse);

        int maxScore = messages.stream().mapToInt(PriorityMessage::getPriority).max().orElse(0);

        List<PriorityMessage> maxScoreMessages = messages.stream()
            .filter(msg -> msg.getPriority() == maxScore).collect(Collectors.toList());

        PriorityMessage message = maxScoreMessages.stream()
            .min(Comparator.comparingLong(PriorityMessage::getTimestamp)).orElse(null);

        if (message == null) {
            return null;
        }

        String existingMessage = PriorityMessageToRedisApiResponseMapper.convertPriorityMessageToRedisApiResponse(
            message);

        message.setVisibleFrom(
            System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(Constants.VISIBILITY_TIMEOUT));
        message.incrementAttempts();

        deleteMessage(queueUrl, existingMessage, message.getPriority());
        updateExistingMessage(queueUrl, message);

        return message;
    }

    private void deleteMessage(String queueUrl, String message, int priority) {
        String removeUrl =
            Constants.REDIS_CONF.getRedisEndpointUrl() + "/" + Constants.REDIS_ZREM_COMMAND + "/"
                + queueUrl + "/" + priority;
        makePostRequest(removeUrl, message);

    }

    private void updateExistingMessage(String queueUrl, PriorityMessage message) {
        String updatedMessage = PriorityMessageToRedisApiResponseMapper.convertPriorityMessageToRedisApiResponse(
            message);
        makePostRequest(
            Constants.REDIS_CONF.getRedisEndpointUrl() + "/" + Constants.REDIS_ZADD_COMMAND + "/"
                + queueUrl + "/" + message.getPriority(), updatedMessage);
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        String apiResponse = makeGetRequest(
            Constants.REDIS_CONF.getRedisEndpointUrl() + "/" + Constants.REDIS_REV_RANGE_COMMAND
                + "/" + queueUrl + "/0/-1");
        List<PriorityMessage> messages = createMessageFromRedisResponse(apiResponse);
        messages.forEach(message -> {
            if (message.getReceiptId().equals(receiptId) && !message.isVisibleAt(
                System.currentTimeMillis())) {
                String foundMessage = PriorityMessageToRedisApiResponseMapper.convertPriorityMessageToRedisApiResponse(
                    message);
                deleteMessage(queueUrl, foundMessage, message.getPriority());
            }
        });
    }

    private int extractPriorityFromJson(String msgBody) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            RedisMessageBody messageBody = mapper.readValue(msgBody, RedisMessageBody.class);
            return messageBody.getScore();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    // this method creates a Message object from response received from redis.
    private List<PriorityMessage> createMessageFromRedisResponse(String response) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            RedisApiResponse apiResponse = mapper.readValue(response, RedisApiResponse.class);
            ArrayList<PriorityMessage> messages = new ArrayList<>();
            for (String line : apiResponse.getResult()) {
                RedisMessageBody redisMessageBody = mapper.readValue(line, RedisMessageBody.class);
                PriorityMessage msg = new PriorityMessage(
                    redisMessageBody.getMessage().getMsgBody(),
                    redisMessageBody.getMessage().getReceiptId(), redisMessageBody.getScore(),
                    redisMessageBody.getTimestamp(), redisMessageBody.getMessage().getAttempts(),
                    redisMessageBody.getMessage().getVisibleFrom());
                messages.add(msg);
            }
            return messages;
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse JSON response", e);
        }
    }

    private String makeGetRequest(String apiUrl) {
        CloseableHttpClient client = HttpClients.createDefault();

        HttpGet httpGet = new HttpGet(apiUrl);
        httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("Authorization",
            String.format("Bearer %s", Constants.REDIS_CONF.getRedisApiToken()));

        try {
            HttpResponse response = client.execute(httpGet);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException(String.format("Redis Api failed with status %s",
                    response.getStatusLine().getStatusCode()));
            }
            String responseEntity = EntityUtils.toString(response.getEntity());
            return responseEntity;
        } catch (Exception e) {
            throw new RuntimeException("Unable to call Redis api", e);
        }
    }

    private String makePostRequest(String apiUrl, String msgBody) {
        CloseableHttpClient client = HttpClients.createDefault();

        HttpPost httpPost = new HttpPost(apiUrl);

        try {
            httpPost.setEntity(new StringEntity(msgBody));
        } catch (Exception e) {
            throw new RuntimeException("Unable to call Redis api", e);
        }

        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("Authorization",
            String.format("Bearer %s", Constants.REDIS_CONF.getRedisApiToken()));

        try {
            CloseableHttpResponse response = client.execute(httpPost);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new RuntimeException(String.format("Redis Api failed with status %s",
                    response.getStatusLine().getStatusCode()));
            }
            return EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            throw new RuntimeException("Unable to call Redis api", e);
        }
    }
}