package com.example.utils;

import com.example.constants.Constants;
import com.example.constants.RedisConf;
import com.example.queueexception.RedisQueueException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Utils {

    private static Properties getConfPropertyObject() {
        Properties confInfo = new Properties();
        InputStream inStream = Utils.class.getClassLoader()
            .getResourceAsStream(Constants.PROP_FILENAME);
        try {
            confInfo.load(inStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return confInfo;
    }

    public static long getVisibilityTimeout() {
        Properties confInfo = getConfPropertyObject();
        return Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    public static RedisConf getRedisConfig() {
        Properties confInfo = getConfPropertyObject();

        String redisEndPointUrl = confInfo.getProperty("redisEndpointUrl");
        String redisApiToken = confInfo.getProperty("redisApiToken");

        return new RedisConf(redisEndPointUrl, redisApiToken);
    }

    public static String updateRedisMessageFields(String json) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Object> jsonMap = objectMapper.readValue(json, Map.class);

        Map<String, Object> messageMap = (Map<String, Object>) jsonMap.get("message");
        if (messageMap == null) {
            throw new RedisQueueException("EXC2", "Message is empty");
        }
        if (!messageMap.containsKey("attempts")) {
            messageMap.put("attempts", 0);
        }
        if (!messageMap.containsKey("visibleFrom")) {
            messageMap.put("visibleFrom", 0);
        }
        if (!messageMap.containsKey("receiptId")) {
            messageMap.put("receiptId", UUID.randomUUID().toString());
        }

        if (!jsonMap.containsKey("score")) {
            jsonMap.put("score", 0);
        }
        if (!jsonMap.containsKey("timestamp")) {
            jsonMap.put("timestamp", System.currentTimeMillis());
        }

        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonMap);
    }
}
