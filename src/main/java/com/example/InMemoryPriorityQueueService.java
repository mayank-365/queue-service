package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryPriorityQueueService implements QueueService {
    private final Map<String, Queue<PriorityMessage>> queues;

    private long visibilityTimeout;

    public InMemoryPriorityQueueService() {
        this.queues = new ConcurrentHashMap<>();

        String propFileName = "config.properties";
        Properties confInfo = new Properties();

        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            confInfo.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    @Override
    public void push(String queueUrl, String msgBody) {
        Queue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue == null) {
            queue = new PriorityQueue<>(Comparator.comparingInt(PriorityMessage::getPriority)
                    .reversed()
                    .thenComparingLong(PriorityMessage::getTimestamp));
            queues.put(queueUrl, queue);
        }

        queue.add(new PriorityMessage(msgBody, this.extractPriorityFromJson(msgBody), System.currentTimeMillis()));
    }

    @Override
    public PriorityMessage pull(String queueUrl) {
        Queue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue == null || queue.isEmpty()) {
            return null;
        }

        PriorityMessage msg = queue.peek();
        msg.setReceiptId(UUID.randomUUID().toString());
        msg.incrementAttempts();
        msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));

        return msg;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        Queue<PriorityMessage> queue = queues.get(queueUrl);

        if(queue == null || queue.isEmpty()) {
            return;
        }

        long nowTime = now();
        for (Iterator<PriorityMessage> it = queue.iterator(); it.hasNext(); ) {
            Message msg = it.next();
            if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
                it.remove();
                break;
            }
        }
    }

    private int extractPriorityFromJson(String msgBody) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(msgBody);
            JsonNode priorityNode = rootNode.get("priority");
            if (priorityNode != null && priorityNode.isInt()) {
                return priorityNode.intValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0; // Default priority if not found or error occurred
    }

    long now() {
        return System.currentTimeMillis();
    }
}