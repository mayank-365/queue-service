package com.example;

import com.example.constants.Constants;
import com.example.queueexception.QueueException;
import com.example.dto.MessageBody;
import com.example.dto.PriorityMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryPriorityQueueService implements QueueService {

    private final Map<String, Queue<PriorityMessage>> queues;

    public InMemoryPriorityQueueService() {
        this.queues = new HashMap<>();
    }

    @Override
    public void push(String queueUrl, String msgBody) {
        if (queueUrl == null) {
            throw new QueueException("EXC1", "Queue Url Is Null.");
        }

        Queue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue == null) {
            queue = new PriorityBlockingQueue<>(10,
                Comparator.comparingInt(PriorityMessage::getPriority).reversed()
                    .thenComparingLong(PriorityMessage::getTimestamp));
            queues.put(queueUrl, queue);
        }

        queue.add(new PriorityMessage(msgBody, this.extractPriorityFromJson(msgBody),
            System.currentTimeMillis()));
    }

    @Override
    public PriorityMessage pull(String queueUrl) {
        if (queueUrl == null) {
            throw new QueueException("EXC1", "Queue Url Is Null.");
        }

        Queue<PriorityMessage> queue = queues.get(queueUrl);

        if (queue == null || queue.isEmpty()) {
            return null;
        }

        PriorityMessage msg = queue.peek();
        msg.setReceiptId(UUID.randomUUID().toString());
        msg.incrementAttempts();
        msg.setVisibleFrom(
            System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(Constants.VISIBILITY_TIMEOUT));

        return msg;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        if (queueUrl == null) {
            throw new QueueException("EXC1", "Queue Url Is Null.");
        }

        Queue<PriorityMessage> queue = queues.get(queueUrl);

        if (queue == null || queue.isEmpty()) {
            return;
        }

        long nowTime = System.currentTimeMillis();
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
            MessageBody messageBody = mapper.readValue(msgBody, MessageBody.class);
            return messageBody.getPriority();
        } catch (Exception e) {
        }
        return 0;
    }
}